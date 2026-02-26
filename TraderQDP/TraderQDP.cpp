/*!
 * \file TraderQDP.cpp
 * \project	WonderTrader
 *
 * \author YourName
 * \date 2024/01/01
 * 
 * \brief QDP柜台交易通道实现
 */
#include "TraderQDP.h"

#include "../Includes/WTSError.hpp"
#include "../Includes/WTSContractInfo.hpp"
#include "../Includes/WTSSessionInfo.hpp"
#include "../Includes/WTSTradeDef.hpp"
#include "../Includes/WTSDataDef.hpp"
#include "../Includes/WTSVariant.hpp"
#include "../Includes/IBaseDataMgr.h"

#include "../Share/ModuleHelper.hpp"
#include "../Share/decimal.h"

#include <boost/filesystem.hpp>

const char* ENTRUST_SECTION = "entrusts";
const char* ORDER_SECTION = "orders";

// By Wesley @ 2022.01.05
#include "../Share/fmtlib.h"
template<typename... Args>
inline void write_log(ITraderSpi* sink, WTSLogLevel ll, const char* format, const Args&... args)
{
    if (sink == NULL)
        return;

    const char* buffer = fmtutil::format(format, args...);
    sink->handleTraderLog(ll, buffer);
}

// InvestorID和InvestorIDNum的转换关系
int InvestorIDToNum(const char *investorID)
{
	char buffer[20] = { 0 };
	size_t i = 0;

	const size_t valid_length = strlen(investorID);
	if (valid_length > 19 || valid_length == 0)
	{
		printf("invalid investorID: %s\n", investorID);
		return 0;
	}

	// 预处理：替换非数字字符为'0'，并复制到缓冲区
	for (i = 0; i < valid_length; ++i)
	{
		if (investorID[i] < '0' || investorID[i] > '9')
		{
			buffer[i] = '0';
		}
		else
		{
			buffer[i] = investorID[i];
		}
	}
	buffer[i] = '\0'; // 确保字符串终止

	// 确定起始位置（长度>9时取后8位）
	const char *start = buffer;
	if (valid_length > 9)
	{
		start += valid_length - 8;
	}

	// 高性能转换：直接计算整数值
	int result = 0;
	for (const char *p = start; *p != '\0'; ++p)
	{
		result = result * 10 + (*p - '0');
	}

	return result;
}

extern "C"
{
    EXPORT_FLAG ITraderApi* createTrader()
    {
        TraderQDP *instance = new TraderQDP();
        return instance;
    }

    EXPORT_FLAG void deleteTrader(ITraderApi* &trader)
    {
        if (NULL != trader)
        {
            delete trader;
            trader = NULL;
        }
    }
}

TraderQDP::TraderQDP()
    : m_pUserAPI(NULL)
    , m_mapPosition(NULL)
    , m_ayOrders(NULL)
    , m_ayTrades(NULL)
    , m_ayFunds(NULL)
    , m_wrapperState(WS_NOTLOGIN)
    , m_uLastQryTime(0)
    , m_iRequestID(0)
    , m_bQuickStart(false)
    , m_bInQuery(false)
    , m_bStopped(false)
    , m_lastQryTime(0)
    , m_sessionID(0)
{
}

TraderQDP::~TraderQDP()
{
}

bool TraderQDP::init(WTSVariant* params)
{
    m_strFront = params->getCString("front");
    m_strBroker = params->getCString("broker");
    m_strUser = params->getCString("user");
    m_strPass = params->getCString("pass");

    m_strAppID = params->getCString("appid");
    m_strAuthCode = params->getCString("authcode");

    m_strFlowDir = params->getCString("flowdir");
    if (m_strFlowDir.empty())
        m_strFlowDir = "QDPTDFlow";

    m_strFlowDir = StrUtil::standardisePath(m_strFlowDir);

    std::string module = params->getCString("qdpmodule");
    if (module.empty())
        module = "qdptraderapi";

    m_strModule = getBinDir() + DLLHelper::wrap_module(module.c_str(), "lib");

    m_hInstQDP = DLLHelper::load_library(m_strModule.c_str());
    
#ifdef _WIN32
#	ifdef _WIN64
    const char* creatorName = "?CreateFtdcTraderApi@CQdpFtdcTraderApi@@SAPEAV1@PEBD@Z";
#	else
    const char* creatorName = "?CreateFtdcTraderApi@CQdpFtdcTraderApi@@SAPAV1@PBD@Z";
#	endif
#else
    const char* creatorName = "_ZN17CQdpFtdcTraderApi19CreateFtdcTraderApiEPKc";
#endif

    m_funcCreator = (QDPCreator)DLLHelper::get_symbol(m_hInstQDP, creatorName);
    m_bQuickStart = params->getBoolean("quick");

    return true;
}

void TraderQDP::release()
{
    if (m_pUserAPI)
    {
        m_pUserAPI->Release();
        m_pUserAPI = NULL;
    }

    if (m_ayOrders)
        m_ayOrders->clear();

    if (m_mapPosition)
        m_mapPosition->clear();

    if (m_ayTrades)
        m_ayTrades->clear();

    if (m_ayFunds)
        m_ayFunds->clear();
}

void TraderQDP::registerSpi(ITraderSpi *listener)
{
    m_sink = listener;
    if (m_sink)
    {
        m_bdMgr = listener->getBaseDataMgr();
    }
}

void TraderQDP::connect()
{
    std::stringstream ss;
    ss << m_strFlowDir << "flows/" << m_strBroker << "/" << m_strUser << "/";
    boost::filesystem::create_directories(ss.str().c_str());
    
    m_pUserAPI = m_funcCreator(ss.str().c_str());
    m_pUserAPI->RegisterSpi(this);
    
    if (m_bQuickStart)
    {
        m_pUserAPI->SubscribePrivateTopic(QDP_TERT_QUICK);
        m_pUserAPI->SubscribePublicTopic(QDP_TERT_QUICK);
    }
    else
    {
        m_pUserAPI->SubscribePrivateTopic(QDP_TERT_RESUME);
        m_pUserAPI->SubscribePublicTopic(QDP_TERT_RESUME);
    }

    m_pUserAPI->RegisterFront((char*)m_strFront.c_str());
    m_pUserAPI->Init();

    if (m_thrdWorker == NULL)
    {
        m_thrdWorker.reset(new StdThread([this]() {
            while (!m_bStopped)
            {
                if (m_queQuery.empty() || m_bInQuery)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }

                uint64_t curTime = TimeUtils::getLocalTimeNow();
                if (curTime - m_lastQryTime < 1000)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    continue;
                }

                m_bInQuery = true;
                CommonExecuter& handler = m_queQuery.front();
                handler();

                {
                    StdUniqueLock lock(m_mtxQuery);
                    m_queQuery.pop();
                }

                m_lastQryTime = TimeUtils::getLocalTimeNow();
                m_bInQuery = false;
            }
        }));
    }
}

void TraderQDP::disconnect()
{
    m_bStopped = true;
    
    if (m_thrdWorker)
    {
        m_thrdWorker->join();
        m_thrdWorker = NULL;
    }
    
    release();
}

bool TraderQDP::isConnected()
{
    return (m_wrapperState == WS_ALLREADY);
}

bool TraderQDP::makeEntrustID(char* buffer, int length)
{
    if (buffer == NULL || length == 0)
        return false;

    try
    {
        memset(buffer, 0, length);
        uint32_t orderref = m_orderRef.fetch_add(1) + 1;
        fmt::format_to(buffer, "{:010d}#{:06d}", (uint32_t)m_sessionID, orderref);
        return true;
    }
    catch (...)
    {
    }

    return false;
}

int TraderQDP::login(const char* user, const char* pass, const char* productInfo)
{
    m_strUser = user;
    m_strPass = pass;
    m_strProdInfo = productInfo;

    if (m_pUserAPI == NULL)
        return -1;

    m_wrapperState = WS_LOGINING;
    
    if (!m_strAppID.empty() && !m_strAuthCode.empty())
        authenticate();
    else
        doLogin();

    return 0;
}

int TraderQDP::authenticate()
{
    CQdpFtdcAuthenticateField req;
    memset(&req, 0, sizeof(req));
    strcpy(req.BrokerID, m_strBroker.c_str());
    strcpy(req.UserID, m_strUser.c_str());
    strcpy(req.AppID, m_strAppID.c_str());
    strcpy(req.AuthCode, m_strAuthCode.c_str());
    strcpy(req.UserProductInfo, m_strProdInfo.c_str());
    
    int iResult = m_pUserAPI->ReqAuthenticate(&req, genRequestID());
    if (iResult != 0)
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Sending authenticate request failed: {}", iResult);
    }

    return iResult;
}

int TraderQDP::doLogin()
{
    CQdpFtdcReqUserLoginField req;
    memset(&req, 0, sizeof(req));
    strcpy(req.BrokerID, m_strBroker.c_str());
    strcpy(req.UserID, m_strUser.c_str());
    strcpy(req.Password, m_strPass.c_str());
    strcpy(req.UserProductInfo, m_strProdInfo.c_str());
    
    int iResult = m_pUserAPI->ReqUserLogin(&req, genRequestID());
    if (iResult != 0)
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Sending login request failed: {}", iResult);
    }

    return iResult;
}

int TraderQDP::logout()
{
    if (m_pUserAPI == NULL)
        return -1;

    CQdpFtdcReqUserLogoutField req;
    memset(&req, 0, sizeof(req));
    strcpy(req.BrokerID, m_strBroker.c_str());
    strcpy(req.UserID, m_strUser.c_str());
    
    int iResult = m_pUserAPI->ReqUserLogout(&req, genRequestID());
    if (iResult != 0)
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Sending logout request failed: {}", iResult);
    }

    return iResult;
}

int TraderQDP::orderInsert(WTSEntrust* entrust)
{
    if (m_pUserAPI == NULL || m_wrapperState != WS_ALLREADY)
    {
        printf("[TraderQDP] Order inserting failed, UserAPI:{%x}, State:{%d}\n",
            m_pUserAPI, m_wrapperState);
        return -1;
    }

    CQdpFtdcInputOrderField req;
    memset(&req, 0, sizeof(req));
	//req.InvestorIDNum = atoi(m_strBroker.c_str());
	req.InvestorIDNum = InvestorIDToNum(m_strUser.c_str());
    // 设置基本参数
	//int idnum = m_mapInstrumentIDtoNum.at(entrust->getCode());
 //   req.InstrumentIDNum = idnum;
	auto iter = m_mapInstrumentIDtoNum.find(entrust->getCode());
	if (iter != m_mapInstrumentIDtoNum.end())
	{
		req.InstrumentIDNum = iter->second;
	}
	else
	{
		write_log(m_sink, LL_ERROR, "[TraderQDP] Order inserting failed: {}", "cant find InstrumentIDNum");
	}
    
    uint32_t orderref = m_orderRef.fetch_add(1);
    if (strlen(entrust->getUserTag()) == 0)
    {
        req.UserOrderLocalID = orderref;
    }
    else
    {
        uint32_t sid, orderref;
        extractEntrustID(entrust->getEntrustID(), sid, orderref);
        req.UserOrderLocalID = orderref;
    }

    if (strlen(entrust->getUserTag()) > 0)
    {
        m_eidCache.put(entrust->getEntrustID(), entrust->getUserTag(), 0, [this](const char* message) {
            write_log(m_sink, LL_WARN, message);
        });
    }

    // WTSContractInfo* ct = entrust->getContractInfo();
    // if (ct == NULL)
    //     return -1;

    // 设置价格类型、方向、开平标志等
    req.OrderPriceType = wrapPriceType(entrust->getPriceType());
    req.Direction = wrapDirectionType(entrust->getDirection(), entrust->getOffsetType());
    req.OffsetFlag = wrapOffsetType(entrust->getOffsetType());
    req.HedgeFlag = QDP_FTDC_CHF_Speculation; // 默认投机
    
    req.LimitPrice = entrust->getPrice();
    req.Volume = (int)entrust->getVolume();
    
    // 设置时间条件和成交量条件
    if (entrust->getOrderFlag() == WOF_NOR)
    {
        req.TimeCondition = QDP_FTDC_TC_GFD;
        req.VolumeCondition = QDP_FTDC_VC_AV;
    }
    else if (entrust->getOrderFlag() == WOF_FAK)
    {
        req.TimeCondition = QDP_FTDC_TC_IOC;
        req.VolumeCondition = QDP_FTDC_VC_AV;
    }
    else if (entrust->getOrderFlag() == WOF_FOK)
    {
        req.TimeCondition = QDP_FTDC_TC_IOC;
        req.VolumeCondition = QDP_FTDC_VC_CV;
    }

    int iResult = m_pUserAPI->ReqOrderInsert(&req, genRequestID());
    if (iResult != 0)
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Order inserting failed: {}", iResult);
    }

    return iResult;
}

int TraderQDP::orderAction(WTSEntrustAction* action)
{
    if (m_wrapperState != WS_ALLREADY)
        return -1;

    uint32_t sessionid, orderref;
    if (!extractEntrustID(action->getEntrustID(), sessionid, orderref))
        return -1;

    CQdpFtdcOrderActionField req;
    memset(&req, 0, sizeof(req));
    
	strcpy(req.OrderSysID, action->getOrderID());
    req.UserOrderLocalID = orderref;
    req.ActionFlag = QDP_FTDC_AF_Delete; // 删除委托
    
    strcpy(req.ExchangeID, action->getExchg());

    int iResult = m_pUserAPI->ReqOrderAction(&req, genRequestID());
    if (iResult != 0)
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Sending cancel request failed: {}", iResult);
    }

    return iResult;
}

int TraderQDP::queryAccount()
{
    if (m_pUserAPI == NULL || m_wrapperState != WS_ALLREADY)
        return -1;

    {
        StdUniqueLock lock(m_mtxQuery);
        m_queQuery.push([this]() {
            CQdpFtdcQryInvestorAccountField req;
            memset(&req, 0, sizeof(req));
            strcpy(req.BrokerID, m_strBroker.c_str());
            strcpy(req.UserID, m_strUser.c_str());
            strcpy(req.InvestorID, m_strUser.c_str());
            m_pUserAPI->ReqQryInvestorAccount(&req, genRequestID());
        });
    }

    return 0;
}

int TraderQDP::queryPositions()
{
    if (m_pUserAPI == NULL || m_wrapperState != WS_ALLREADY)
        return -1;

    {
        StdUniqueLock lock(m_mtxQuery);
        m_queQuery.push([this]() {
            CQdpFtdcQryInvestorPositionField req;
            memset(&req, 0, sizeof(req));
            strcpy(req.BrokerID, m_strBroker.c_str());
            strcpy(req.UserID, m_strUser.c_str());
            strcpy(req.InvestorID, m_strUser.c_str());
            m_pUserAPI->ReqQryInvestorPosition(&req, genRequestID());
        });
    }

    return 0;
}

int TraderQDP::queryOrders()
{
    if (m_pUserAPI == NULL || m_wrapperState != WS_ALLREADY)
        return -1;

    {
        StdUniqueLock lock(m_mtxQuery);
        m_queQuery.push([this]() {
            CQdpFtdcQryOrderField req;
            memset(&req, 0, sizeof(req));
            strcpy(req.BrokerID, m_strBroker.c_str());
            strcpy(req.UserID, m_strUser.c_str());
            strcpy(req.InvestorID, m_strUser.c_str());
            m_pUserAPI->ReqQryOrder(&req, genRequestID());
        });
    }

    return 0;
}

int TraderQDP::queryTrades()
{
    if (m_pUserAPI == NULL || m_wrapperState != WS_ALLREADY)
        return -1;

    {
        StdUniqueLock lock(m_mtxQuery);
        m_queQuery.push([this]() {
            CQdpFtdcQryTradeField req;
            memset(&req, 0, sizeof(req));
            strcpy(req.BrokerID, m_strBroker.c_str());
            strcpy(req.UserID, m_strUser.c_str());
            strcpy(req.InvestorID, m_strUser.c_str());
            m_pUserAPI->ReqQryTrade(&req, genRequestID());
        });
    }

    return 0;
}

// QDP回调函数实现
void TraderQDP::OnFrontConnected()
{
    write_log(m_sink, LL_INFO, "[TraderQDP] Front connected");
    if (m_sink)
        m_sink->handleEvent(WTE_Connect, 0);
}

void TraderQDP::OnFrontDisconnected(int nReason)
{
    write_log(m_sink, LL_ERROR, "[TraderQDP] Front disconnected, reason: {}", nReason);
    m_wrapperState = WS_NOTLOGIN;
    if (m_sink)
        m_sink->handleEvent(WTE_Close, nReason);
}

void TraderQDP::OnHeartBeatWarning(int nTimeLapse)
{
	write_log(m_sink, LL_DEBUG, "[TraderQDP][{}-{}] Heartbeating...", m_strBroker.c_str(), m_strUser.c_str());
}

void TraderQDP::OnRspAuthenticate(CQdpFtdcRtnAuthenticateField *pRtnAuthenticate, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (!IsErrorRspInfo(pRspInfo))
    {
        write_log(m_sink, LL_INFO, "[TraderQDP] Authentication succeed");
        doLogin();
    }
    else
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP] Authentication failed: {}", pRspInfo->ErrorMsg);
        m_wrapperState = WS_LOGINFAILED;
        if (m_sink)
            m_sink->onLoginResult(false, pRspInfo->ErrorMsg, 0);
    }
}

void TraderQDP::OnRspUserLogin(CQdpFtdcRspUserLoginField *pRspUserLogin, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (!IsErrorRspInfo(pRspInfo))
    {
        m_wrapperState = WS_LOGINED;
        m_sessionID = pRspUserLogin->SessionID;
        m_orderRef = pRspUserLogin->MaxOrderLocalID;
        
        // 获取当前交易日
        m_lDate = atoi(m_pUserAPI->GetTradingDay());

        write_log(m_sink, LL_INFO, "[TraderQDP][{}-{}] Login succeed, SessionID: {}", 
            m_strBroker.c_str(), m_strUser.c_str(), m_sessionID);

        // 初始化缓存器
        std::stringstream ss;
        ss << m_strFlowDir << "local/" << m_strBroker << "/";
        std::string path = StrUtil::standardisePath(ss.str());
        if (!StdFile::exists(path.c_str()))
            boost::filesystem::create_directories(path.c_str());
        
        // 初始化委托单缓存器
        ss << m_strUser << "_eid.sc";
        m_eidCache.init(ss.str().c_str(), m_lDate, [this](const char* message) {
            write_log(m_sink, LL_WARN, message);
        });

        // 初始化订单标记缓存器
        ss.str("");
        ss << m_strFlowDir << "local/" << m_strBroker << "/" << m_strUser << "_oid.sc";
        m_oidCache.init(ss.str().c_str(), m_lDate, [this](const char* message) {
            write_log(m_sink, LL_WARN, message);
        });

        write_log(m_sink, LL_INFO, "[TraderQDP][{}-{}] Login succeed, trading date: {}", 
            m_strBroker.c_str(), m_strUser.c_str(), m_lDate);
        
        m_wrapperState = WS_ALLREADY;
        if (m_sink)
            m_sink->onLoginResult(true, "", m_lDate);

        /// 准备就绪 QDP_TERT_PRIVATE 私有流; QDP_TERT_PUBLIC 公有流;
        CQdpFtdcFlowStatusField ftdField1;
        memset(&ftdField1, 0, sizeof(CQdpFtdcFlowStatusField));
        ftdField1.SequenceSeries = QDP_TERT_PRIVATE;
        ftdField1.bReady = true;
        m_pUserAPI->ReqReady(&ftdField1, 0);
        memset(&ftdField1, 0, sizeof(CQdpFtdcFlowStatusField));
        ftdField1.SequenceSeries = QDP_TERT_PUBLIC;
        ftdField1.bReady = true;
        m_pUserAPI->ReqReady(&ftdField1, 0);

		CQdpFtdcQryInstrumentField qry;
		memset(&qry, 0, sizeof(qry));
		m_pUserAPI->ReqQryInstrument(&qry, 0);
    }
    else
    {
        write_log(m_sink, LL_ERROR, "[TraderQDP][{}-{}] Login failed: {}", 
            m_strBroker.c_str(), m_strUser.c_str(), pRspInfo->ErrorMsg);
        m_wrapperState = WS_LOGINFAILED;
        if (m_sink)
            m_sink->onLoginResult(false, pRspInfo->ErrorMsg, 0);
    }
}

void TraderQDP::OnRspUserLogout(CQdpFtdcRspUserLogoutField *pRspUserLogout, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    m_wrapperState = WS_NOTLOGIN;
    if (m_sink)
        m_sink->handleEvent(WTE_Logout, 0);
}

void TraderQDP::OnRspOrderInsert(CQdpFtdcRspInputOrderField *pRspInputOrder, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInputOrder)
    {
        WTSEntrust* entrust = makeEntrust(pRspInputOrder);
        if (entrust)
        {
            WTSError *err = makeError(pRspInfo);
            if (m_sink)
                m_sink->onRspEntrust(entrust, err);
            entrust->release();
            err->release();
        }
    }
}

void TraderQDP::OnRspOrderAction(CQdpFtdcOrderActionField *pOrderAction, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (IsErrorRspInfo(pRspInfo))
    {
        WTSError* error = WTSError::create(WEC_ORDERCANCEL, pRspInfo->ErrorMsg);
        if (m_sink)
            m_sink->onTraderError(error);
        error->release();
    }
}

void TraderQDP::OnRspQryInvestorAccount(CQdpFtdcRspInvestorAccountField *pRspInvestorAccount, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (bIsLast)
    {
        m_bInQuery = false;
    }

    if (!IsErrorRspInfo(pRspInfo) && pRspInvestorAccount)
    {
        if (NULL == m_ayFunds)
            m_ayFunds = WTSArray::create();

        WTSAccountInfo* accountInfo = makeAccountInfo(pRspInvestorAccount);
        if (accountInfo)
        {
            m_ayFunds->append(accountInfo, false);
        }
    }

    if(bIsLast)
    {
        if (m_sink)
            m_sink->onRspAccount(m_ayFunds);

        if (m_ayFunds)
            m_ayFunds->clear();
    }
}

void TraderQDP::OnRspQryInvestorPosition(CQdpFtdcRspInvestorPositionField *pRspInvestorPosition, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (bIsLast)
    {
        m_bInQuery = false;
    }

    if (!IsErrorRspInfo(pRspInfo) && pRspInvestorPosition)
    {
        if (NULL == m_mapPosition)
            m_mapPosition = PositionMap::create();

        WTSPositionItem* pos = makePositionInfo(pRspInvestorPosition);
        if (pos)
        {
            std::string key = fmt::format("{}-{}", pRspInvestorPosition->InstrumentID, pRspInvestorPosition->Direction);
            m_mapPosition->add(key, pos, false);
        }
    }

    if (bIsLast)
    {
        WTSArray* ayPos = WTSArray::create();

        if(m_mapPosition && m_mapPosition->size() > 0)
        {
            for (auto it = m_mapPosition->begin(); it != m_mapPosition->end(); it++)
            {
                ayPos->append(it->second, true);
            }
        }

        if (m_sink)
            m_sink->onRspPosition(ayPos);

        if (m_mapPosition)
        {
            m_mapPosition->release();
            m_mapPosition = NULL;
        }

        ayPos->release();
    }
}

void TraderQDP::OnRspQryTrade(CQdpFtdcTradeField *pTrade, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (bIsLast)
    {
        m_bInQuery = false;
    }

    if (!IsErrorRspInfo(pRspInfo) && pTrade)
    {
        if (NULL == m_ayTrades)
            m_ayTrades = WTSArray::create();

        WTSTradeInfo* trade = makeTradeRecord(pTrade);
        if (trade)
        {
            m_ayTrades->append(trade, false);
        }
    }

    if (bIsLast)
    {
        if (m_sink)
            m_sink->onRspTrades(m_ayTrades);

        if (NULL != m_ayTrades)
            m_ayTrades->clear();
    }
}

void TraderQDP::OnRspQryOrder(CQdpFtdcOrderField *pOrder, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (bIsLast)
    {
        m_bInQuery = false;
    }

    if (!IsErrorRspInfo(pRspInfo) && pOrder)
    {
        if (NULL == m_ayOrders)
            m_ayOrders = WTSArray::create();

        WTSOrderInfo* orderInfo = makeOrderInfo(pOrder);
        if (orderInfo)
        {
            m_ayOrders->append(orderInfo, false);
        }
    }

    if (bIsLast)
    {
        if (m_sink)
            m_sink->onRspOrders(m_ayOrders);

        if (m_ayOrders)
            m_ayOrders->clear();
    }
}

void TraderQDP::OnRtnOrder(CQdpFtdcOrderField *pOrder)
{
    WTSOrderInfo *orderInfo = makeOrderInfo(pOrder);
    if (orderInfo)
    {
        if (m_sink)
            m_sink->onPushOrder(orderInfo);

        orderInfo->release();
    }
}

void TraderQDP::OnRtnTrade(CQdpFtdcTradeField *pTrade)
{
    WTSTradeInfo *tRecord = makeTradeRecord(pTrade);
    if (tRecord)
    {
        if (m_sink)
            m_sink->onPushTrade(tRecord);

        tRecord->release();
    }
}

void TraderQDP::OnErrRtnOrderInsert(CQdpFtdcRspInputOrderField *pRspInputOrder, CQdpFtdcRspInfoField *pRspInfo)
{
    if (pRspInputOrder)
    {
        WTSEntrust* entrust = makeEntrust(pRspInputOrder);
        if (entrust)
        {
            WTSError *err = makeError(pRspInfo);
            if (m_sink)
                m_sink->onRspEntrust(entrust, err);
            entrust->release();
            err->release();
        }
    }
}

void TraderQDP::OnErrRtnOrderAction(CQdpFtdcOrderActionField *pOrderAction, CQdpFtdcRspInfoField *pRspInfo)
{
    // 处理撤单错误
    if (IsErrorRspInfo(pRspInfo))
    {
        WTSError* error = WTSError::create(WEC_ORDERCANCEL, pRspInfo->ErrorMsg);
        if (m_sink)
            m_sink->onTraderError(error);
        error->release();
    }
}

void TraderQDP::OnRtnInstrumentStatus(CQdpFtdcInstrumentStatusField *pInstrumentStatus)
{
    if (m_sink)
        m_sink->onPushInstrumentStatus(pInstrumentStatus->ExchangeID, pInstrumentStatus->InstrumentID, (WTSTradeStatus)pInstrumentStatus->InstrumentStatus);
}

void TraderQDP::OnRspQryInstrument(CQdpFtdcRspInstrumentField *pRspInstrument, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
	if (pRspInfo->ErrorID == 0)
	{
		m_mapInstrumentIDtoNum.emplace(pRspInstrument->InstrumentID, pRspInstrument->InstrumentIDNum);
	}
}

bool TraderQDP::IsErrorRspInfo(CQdpFtdcRspInfoField *pRspInfo)
{
    if (pRspInfo && pRspInfo->ErrorID != 0)
        return true;
    return false;
}

// 类型转换函数
char TraderQDP::wrapPriceType(WTSPriceType priceType)
{
    switch (priceType)
    {
    case WPT_ANYPRICE:
        return QDP_FTDC_OPT_AnyPrice;
    case WPT_LIMITPRICE:
        return QDP_FTDC_OPT_LimitPrice;
    case WPT_BESTPRICE:
        return QDP_FTDC_OPT_BestPrice;
    default:
        return QDP_FTDC_OPT_LimitPrice;
    }
}

char TraderQDP::wrapDirectionType(WTSDirectionType dirType, WTSOffsetType offType)
{
    if (dirType == WDT_LONG)
        return (offType == WOT_OPEN) ? QDP_FTDC_D_Buy : QDP_FTDC_D_Sell;
    else
        return (offType == WOT_OPEN) ? QDP_FTDC_D_Sell : QDP_FTDC_D_Buy;
}

char TraderQDP::wrapOffsetType(WTSOffsetType offType)
{
    switch (offType)
    {
    case WOT_OPEN:
        return QDP_FTDC_OF_Open;
    case WOT_CLOSE:
        return QDP_FTDC_OF_Close;
    case WOT_CLOSETODAY:
        return QDP_FTDC_OF_CloseToday;
    case WOT_CLOSEYESTERDAY:
        return QDP_FTDC_OF_CloseYesterday;
    default:
        return QDP_FTDC_OF_Open;
    }
}

char TraderQDP::wrapTimeCondition(WTSTimeCondition timeCond)
{
    switch (timeCond)
    {
    case WTC_IOC:
        return QDP_FTDC_TC_IOC;
    case WTC_GFD:
        return QDP_FTDC_TC_GFD;
    default:
        return QDP_FTDC_TC_GFD;
    }
}

WTSDirectionType TraderQDP::wrapDirectionType(char dirType, char offType)
{
    if (dirType == QDP_FTDC_D_Buy)
        return (offType == QDP_FTDC_OF_Open) ? WDT_LONG : WDT_SHORT;
    else
        return (offType == QDP_FTDC_OF_Open) ? WDT_SHORT : WDT_LONG;
}

WTSOffsetType TraderQDP::wrapOffsetType(char offType)
{
    switch (offType)
    {
    case QDP_FTDC_OF_Open:
        return WOT_OPEN;
    case QDP_FTDC_OF_Close:
        return WOT_CLOSE;
    case QDP_FTDC_OF_CloseToday:
        return WOT_CLOSETODAY;
    case QDP_FTDC_OF_CloseYesterday:
        return WOT_CLOSEYESTERDAY;
    default:
        return WOT_OPEN;
    }
}

WTSPriceType TraderQDP::wrapPriceType(char priceType)
{
    switch (priceType)
    {
    case QDP_FTDC_OPT_AnyPrice:
        return WPT_ANYPRICE;
    case QDP_FTDC_OPT_LimitPrice:
        return WPT_LIMITPRICE;
    case QDP_FTDC_OPT_BestPrice:
        return WPT_BESTPRICE;
    default:
        return WPT_LIMITPRICE;
    }
}

WTSTimeCondition TraderQDP::wrapTimeCondition(char timeCond)
{
    switch (timeCond)
    {
    case QDP_FTDC_TC_IOC:
        return WTC_IOC;
    case QDP_FTDC_TC_GFD:
        return WTC_GFD;
    default:
        return WTC_GFD;
    }
}

WTSOrderState TraderQDP::wrapOrderState(char orderState)
{
    switch (orderState)
    {
    case QDP_FTDC_OS_AllTraded:
        return WOS_AllTraded;
    case QDP_FTDC_OS_PartTradedQueueing:
        return WOS_PartTraded_Queuing;
    case QDP_FTDC_OS_PartTradedNotQueueing:
        return WOS_PartTraded_NotQueuing;
    case QDP_FTDC_OS_NoTradeQueueing:
        return WOS_NotTraded_Queuing;
    case QDP_FTDC_OS_NoTradeNotQueueing:
        return WOS_NotTraded_NotQueuing;
    case QDP_FTDC_OS_Canceled:
        return WOS_Canceled;
    default:
        return WOS_Submitting;
    }
}

// 数据转换函数
WTSOrderInfo* TraderQDP::makeOrderInfo(CQdpFtdcOrderField* orderField)
{
    WTSContractInfo* contract = m_bdMgr->getContract(orderField->InstrumentID);
    if (contract == NULL)
        return NULL;

    WTSOrderInfo* pRet = WTSOrderInfo::create();
    pRet->setContractInfo(contract);
    pRet->setPrice(orderField->LimitPrice);
    pRet->setVolume(orderField->Volume);
    pRet->setDirection(wrapDirectionType(orderField->Direction, orderField->OffsetFlag));
    pRet->setPriceType(wrapPriceType(orderField->OrderPriceType));
    pRet->setOffsetType(wrapOffsetType(orderField->OffsetFlag));
    
    // 设置订单标志
    if(orderField->TimeCondition == QDP_FTDC_TC_GFD)
    {
        pRet->setOrderFlag(WOF_NOR);
    }
    else if (orderField->TimeCondition == QDP_FTDC_TC_IOC)
    {
        if(orderField->VolumeCondition == QDP_FTDC_VC_AV || orderField->VolumeCondition == QDP_FTDC_VC_MV)
            pRet->setOrderFlag(WOF_FAK);
        else
            pRet->setOrderFlag(WOF_FOK);
    }

    pRet->setVolTraded(orderField->VolumeTraded);
    pRet->setVolLeft(orderField->VolumeRemain);
    pRet->setCode(orderField->InstrumentID);
    pRet->setExchange(orderField->ExchangeID);

    // 设置订单时间
    std::string strTime = orderField->InsertTime;
    StrUtil::replace(strTime, ":", "");
    uint32_t uTime = strtoul(strTime.c_str(), NULL, 10);
    
    pRet->setOrderDate(m_lDate);
    pRet->setOrderTime(TimeUtils::makeTime(pRet->getOrderDate(), uTime * 1000));
    pRet->setOrderState(wrapOrderState(orderField->OrderStatus));
    
    // 生成委托ID
    generateEntrustID(pRet->getEntrustID(), m_sessionID, orderField->UserOrderLocalID);
    pRet->setOrderID(orderField->OrderSysID);

    // 设置用户标签
    const char* usertag = m_eidCache.get(pRet->getEntrustID());
    if (strlen(usertag) == 0)
    {
        pRet->setUserTag(pRet->getEntrustID());
    }
    else
    {
        pRet->setUserTag(usertag);
        
        if (strlen(pRet->getOrderID()) > 0)
        {
            m_oidCache.put(StrUtil::trim(pRet->getOrderID()).c_str(), usertag, 0, [this](const char* message) {
                write_log(m_sink, LL_ERROR, message);
            });
        }
    }

    return pRet;
}

WTSEntrust* TraderQDP::makeEntrust(CQdpFtdcRspInputOrderField *entrustField)
{
	const char * instrumentID = entrustField->InstrumentID;
    WTSContractInfo* ct = m_bdMgr->getContract(instrumentID);
    if (ct == NULL)
        return NULL;

    WTSEntrust* pRet = WTSEntrust::create(
		instrumentID,
        entrustField->Volume,
        entrustField->LimitPrice,
        ct->getExchg());

    pRet->setContractInfo(ct);
    pRet->setDirection(wrapDirectionType(entrustField->Direction, entrustField->OffsetFlag));
    pRet->setPriceType(wrapPriceType(entrustField->OrderPriceType));
    pRet->setOffsetType(wrapOffsetType(entrustField->OffsetFlag));
    
    if (entrustField->TimeCondition == QDP_FTDC_TC_GFD)
    {
        pRet->setOrderFlag(WOF_NOR);
    }
    else if (entrustField->TimeCondition == QDP_FTDC_TC_IOC)
    {
        if (entrustField->VolumeCondition == QDP_FTDC_VC_AV || entrustField->VolumeCondition == QDP_FTDC_VC_MV)
            pRet->setOrderFlag(WOF_FAK);
        else
            pRet->setOrderFlag(WOF_FOK);
    }

    generateEntrustID(pRet->getEntrustID(), m_sessionID, entrustField->UserOrderLocalID);

    const char* usertag = m_eidCache.get(pRet->getEntrustID());
    if (strlen(usertag) > 0)
        pRet->setUserTag(usertag);

    return pRet;
}

WTSError* TraderQDP::makeError(CQdpFtdcRspInfoField* rspInfo)
{
    WTSError* pRet = WTSError::create((WTSErroCode)rspInfo->ErrorID, rspInfo->ErrorMsg);
    return pRet;
}

WTSTradeInfo* TraderQDP::makeTradeRecord(CQdpFtdcTradeField *tradeField)
{
    WTSContractInfo* contract = m_bdMgr->getContract(tradeField->InstrumentID, tradeField->ExchangeID);
    if (contract == NULL)
        return NULL;

    WTSCommodityInfo* commInfo = contract->getCommInfo();
    WTSTradeInfo *pRet = WTSTradeInfo::create(tradeField->InstrumentID, commInfo->getExchg());
    pRet->setContractInfo(contract);
    pRet->setVolume(tradeField->TradeVolume);
    pRet->setPrice(tradeField->TradePrice);
    pRet->setTradeID(tradeField->TradeID);

    // 设置交易时间
    std::string strTime = tradeField->TradeTime;
    StrUtil::replace(strTime, ":", "");
    uint32_t uTime = strtoul(strTime.c_str(), NULL, 10);
    
    pRet->setTradeDate(m_lDate);
    pRet->setTradeTime(TimeUtils::makeTime(m_lDate, uTime * 1000));

    pRet->setDirection(wrapDirectionType(tradeField->Direction, tradeField->OffsetFlag));
    pRet->setOffsetType(wrapOffsetType(tradeField->OffsetFlag));
    pRet->setRefOrder(tradeField->OrderSysID);

    double amount = commInfo->getVolScale() * tradeField->TradeVolume * pRet->getPrice();
    pRet->setAmount(amount);

    const char* usertag = m_oidCache.get(StrUtil::trim(pRet->getRefOrder()).c_str());
    if (strlen(usertag))
        pRet->setUserTag(usertag);

    return pRet;
}

WTSAccountInfo* TraderQDP::makeAccountInfo(CQdpFtdcRspInvestorAccountField* accountField)
{
    WTSAccountInfo* accountInfo = WTSAccountInfo::create();
    accountInfo->setPreBalance(accountField->PreBalance);
    accountInfo->setCloseProfit(accountField->CloseProfit);
    accountInfo->setDynProfit(accountField->PositionProfit);
    accountInfo->setMargin(accountField->Margin);
    accountInfo->setAvailable(accountField->Available);
    accountInfo->setCommission(accountField->Fee);
    accountInfo->setFrozenMargin(accountField->FrozenMargin);
    accountInfo->setFrozenCommission(accountField->FrozenFee);
    accountInfo->setDeposit(accountField->Deposit);
    accountInfo->setWithdraw(accountField->Withdraw);
    accountInfo->setBalance(accountField->Balance);
    accountInfo->setCurrency("CNY");
    
    return accountInfo;
}

WTSPositionItem* TraderQDP::makePositionInfo(CQdpFtdcRspInvestorPositionField* positionField)
{
    WTSContractInfo* contract = m_bdMgr->getContract(positionField->InstrumentID);
    if (contract == NULL)
        return NULL;

    WTSCommodityInfo* commInfo = contract->getCommInfo();
    WTSPositionItem* pos = WTSPositionItem::create(positionField->InstrumentID, commInfo->getCurrency(), commInfo->getExchg());
    pos->setContractInfo(contract);
    
    pos->setDirection(wrapDirectionType(positionField->Direction, positionField->HedgeFlag));
    pos->setNewPosition(positionField->TodayPosition);
    pos->setPrePosition(positionField->Position - positionField->TodayPosition);
    pos->setMargin(positionField->UsedMargin);
    pos->setDynProfit(positionField->PositionProfit);
    pos->setPositionCost(positionField->PositionCost);

    if (pos->getTotalPosition() != 0)
    {
        pos->setAvgPrice(positionField->PositionCost / pos->getTotalPosition() / commInfo->getVolScale());
    }

    return pos;
}

void TraderQDP::generateEntrustID(char* buffer, uint32_t sessionid, uint32_t orderRef)
{
    fmtutil::format_to(buffer, "{:010d}#{:06d}", sessionid, orderRef);
}

bool TraderQDP::extractEntrustID(const char* entrustid, uint32_t &sessionid, uint32_t &orderRef)
{
    thread_local static char buffer[64];
    wt_strcpy(buffer, entrustid);
    char* s = buffer;
    
    auto idx = StrUtil::findFirst(s, '#');
    if (idx == std::string::npos)
        return false;
    s[idx] = '\0';
    sessionid = strtoul(s, NULL, 10);
    s += idx + 1;
    
    orderRef = strtoul(s, NULL, 10);
    return true;
}

uint32_t TraderQDP::genRequestID()
{
    return m_iRequestID.fetch_add(1) + 1;
}