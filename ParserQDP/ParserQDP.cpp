/*!
 * \file ParserQDP.cpp
 * \project	WonderTrader
 *
 * \author Wesley
 * \date 2024/01/15
 * 
 * \brief QDP柜台行情解析器实现
 */
#include "ParserQDP.h"
#include "../Share/StrUtil.hpp"
#include "../Share/StdUtils.hpp"
#include "../Share/TimeUtils.hpp"
#include "../Share/ModuleHelper.hpp"

#include "../Includes/WTSDataDef.hpp"
#include "../Includes/WTSContractInfo.hpp"
#include "../Includes/WTSVariant.hpp"
#include "../Includes/IBaseDataMgr.h"
#include "../Includes/WTSVersion.h"

#include <boost/filesystem.hpp>

// By Wesley @ 2022.01.05
#include "../Share/fmtlib.h"
template<typename... Args>
inline void write_log(IParserSpi* sink, WTSLogLevel ll, const char* format, const Args&... args)
{
    if (sink == NULL)
        return;

    static thread_local char buffer[512] = { 0 };
    fmtutil::format_to(buffer, format, args...);

    sink->handleParserLog(ll, buffer);
}

extern "C"
{
    EXPORT_FLAG IParserApi* createParser()
    {
        ParserQDP* parser = new ParserQDP();
        return parser;
    }

    EXPORT_FLAG void deleteParser(IParserApi* &parser)
    {
        if (NULL != parser)
        {
            delete parser;
            parser = NULL;
        }
    }
};

uint32_t ParserQDP::strToTime(const char* strTime)
{
    std::string str;
    const char *pos = strTime;
    while(strlen(pos) > 0)
    {
        if(pos[0] != ':')
        {
            str.append(pos, 1);
        }
        pos++;
    }
    return strtoul(str.c_str(), NULL, 10);
}

inline double ParserQDP::checkValid(double val)
{
    if (val == DBL_MAX || val == FLT_MAX)
        return 0;
    return val;
}
// inline uint32_t strToTime(const char* strTime)
// {
// 	static char str[10] = { 0 };
// 	const char *pos = strTime;
// 	int idx = 0;
// 	auto len = strlen(strTime);
// 	for(std::size_t i = 0; i < len; i++)
// 	{
// 		if(strTime[i] != ':')
// 		{
// 			str[idx] = strTime[i];
// 			idx++;
// 		}
// 	}
// 	str[idx] = '\0';

// 	return strtoul(str, NULL, 10);
// }

// inline double checkValid(double val)
// {
// 	if (val == DBL_MAX || val == FLT_MAX)
// 		return 0;

// 	return val;
// }

ParserQDP::ParserQDP()
    : m_pUserAPI(NULL)
    , m_iRequestID(0)
    , m_uTradingDate(0)
    , m_loginState(LS_NOTLOGIN)
    , m_sink(NULL)
    , m_pBaseDataMgr(NULL)
    , m_hInstQDP(NULL)
    , m_funcCreator(NULL)
{
}

ParserQDP::~ParserQDP()
{
    release();
}

bool ParserQDP::init(WTSVariant* config)
{
    m_strFrontAddr = config->getCString("front");
    m_strBroker = config->getCString("broker");
    m_strUserID = config->getCString("user");
    m_strPassword = config->getCString("pass");
    m_strFlowDir = config->getCString("flowdir");

    if (m_strFlowDir.empty())
        m_strFlowDir = "QDP_MDFlow";

    m_strFlowDir = StrUtil::standardisePath(m_strFlowDir);

    // 加载QDP动态库
    std::string module = config->getCString("qdpmodule");
    if (module.empty())
        module = "qdmdapi";
    
    std::string dllpath = getBinDir() + DLLHelper::wrap_module(module.c_str(), "lib");
    m_hInstQDP = DLLHelper::load_library(dllpath.c_str());
    
    if (m_hInstQDP == NULL)
    {
        if (m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Failed to load QDP library: {}", dllpath);
        return false;
    }

    // 获取创建函数
#ifdef _WIN32
#	ifdef _WIN64
    const char* creatorName = "?CreateFtdcMduserApi@CQdFtdcMduserApi@@SAPEAV1@PEBD@Z";
#	else
    const char* creatorName = "?CreateFtdcMduserApi@CQdFtdcMduserApi@@SAPAV1@PBD@Z";
#	endif
#else
    const char* creatorName = "_ZN16CQdFtdcMduserApi19CreateFtdcMduserApiEPKc";
#endif
    
    m_funcCreator = (QDPCreator)DLLHelper::get_symbol(m_hInstQDP, creatorName);
    if (m_funcCreator == NULL)
    {
        if (m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Failed to get creator function: {}", creatorName);
        return false;
    }

    // 创建API实例
    std::string path = StrUtil::printf("%s/%s/%s/", m_strFlowDir.c_str(), m_strBroker.c_str(), m_strUserID.c_str());
    if (!StdFile::exists(path.c_str()))
    {
        boost::filesystem::create_directories(boost::filesystem::path(path));
    }

    m_pUserAPI = m_funcCreator(path.c_str());
    if (m_pUserAPI == NULL)
    {
        if (m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Failed to create QDP API instance");
        return false;
    }

    // 注册回调接口
    m_pUserAPI->RegisterSpi(this);
    m_pUserAPI->RegisterFront((char*)m_strFrontAddr.c_str());

    if (m_sink)
        write_log(m_sink, LL_INFO, "[ParserQDP] QDP parser initialized successfully");

    return true;
}

void ParserQDP::release()
{
    disconnect();
    
    if (m_hInstQDP)
    {
        DLLHelper::free_library(m_hInstQDP);
        m_hInstQDP = NULL;
    }
}

bool ParserQDP::connect()
{
    if(m_pUserAPI)
    {
        m_pUserAPI->Init();
        return true;
    }
    return false;
}

bool ParserQDP::disconnect()
{
    if(m_pUserAPI)
    {
        m_pUserAPI->RegisterSpi(NULL);
        m_pUserAPI->Release();
        m_pUserAPI = NULL;
    }
    
    m_loginState = LS_NOTLOGIN;
    return true;
}

bool ParserQDP::isConnected()
{
    return m_pUserAPI != NULL && m_loginState == LS_LOGINED;
}

void ParserQDP::registerSpi(IParserSpi* listener)
{
    m_sink = listener;
    if(m_sink)
        m_pBaseDataMgr = m_sink->getBaseDataMgr();
}

void ParserQDP::subscribe(const CodeSet &vecSymbols)
{
    if(m_uTradingDate == 0)
    {
        m_filterSubs = vecSymbols;
    }
    else
    {
        m_filterSubs = vecSymbols;
        
        if (vecSymbols.empty())
            return;

        char ** subscribe = new char*[vecSymbols.size()];
        int nCount = 0;
        for (auto& code : vecSymbols)
        {
            std::size_t pos = code.find('.');
            if (pos != std::string::npos)
                subscribe[nCount++] = (char*)code.c_str() + pos + 1;
            else
                subscribe[nCount++] = (char*)code.c_str();
        }

        if(m_pUserAPI && nCount > 0)
        {
            int iResult = m_pUserAPI->SubMarketData(subscribe, nCount);
            if(iResult != 0)
            {
                if (m_sink)
                    write_log(m_sink, LL_ERROR, "[ParserQDP] Sending md subscribe request failed: {}", iResult);
            }
            else
            {
                if (m_sink)
                    write_log(m_sink, LL_INFO, "[ParserQDP] Market data of {} contracts subscribed in total", nCount);
            }
        }
        delete[] subscribe;
    }
}

void ParserQDP::unsubscribe(const CodeSet &vecSymbols)
{
    if (!vecSymbols.empty() && m_pUserAPI)
    {
        char ** unsubscribe = new char*[vecSymbols.size()];
        int nCount = 0;
        for (auto& code : vecSymbols)
        {
            std::size_t pos = code.find('.');
            if (pos != std::string::npos)
                unsubscribe[nCount++] = (char*)code.c_str() + pos + 1;
            else
                unsubscribe[nCount++] = (char*)code.c_str();
        }

        int iResult = m_pUserAPI->UnSubMarketData(unsubscribe, nCount);
        if(iResult != 0)
        {
            if (m_sink)
                write_log(m_sink, LL_ERROR, "[ParserQDP] Sending md unsubscribe request failed: {}", iResult);
        }
        delete[] unsubscribe;
    }
}

// QDP回调函数实现
void ParserQDP::OnFrontConnected()
{
    if(m_sink)
    {
        write_log(m_sink, LL_INFO, "[ParserQDP] Market data server connected");
        m_sink->handleEvent(WPE_Connect, 0);
    }

    ReqUserLogin();
}

void ParserQDP::OnFrontDisconnected(int nReason)
{
    if(m_sink)
    {
        write_log(m_sink, LL_ERROR, "[ParserQDP] Market data server disconnected: {}", nReason);
        m_sink->handleEvent(WPE_Close, 0);
    }
    m_loginState = LS_NOTLOGIN;
}

void ParserQDP::OnHeartBeatWarning(int nTimeLapse)
{
    if(m_sink)
        write_log(m_sink, LL_INFO, "[ParserQDP] Heartbeating, elapse: {}", nTimeLapse);
}

void ParserQDP::OnRspError(CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if(pRspInfo && pRspInfo->ErrorID != 0)
    {
        if(m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Error response: ErrorID={}, ErrorMsg={}", 
                     pRspInfo->ErrorID, pRspInfo->ErrorMsg);
    }
}

void ParserQDP::OnRspUserLogin(CQdFtdcRspUserLoginField *pRspUserLogin, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if(bIsLast && !IsErrorRspInfo(pRspInfo))
    {
        m_uTradingDate = strtoul(pRspUserLogin->TradingDay, NULL, 10);
        m_loginState = LS_LOGINED;
        
        if(m_sink)
        {
            write_log(m_sink, LL_INFO, "[ParserQDP] User login successfully, trading day: {}", m_uTradingDate);
            m_sink->handleEvent(WPE_Login, 0);
        }

        // 订阅行情数据
        SubscribeMarketData();
    }
}

void ParserQDP::OnRspUserLogout(CQdFtdcRspUserLogoutField *pRspUserLogout, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if(!IsErrorRspInfo(pRspInfo))
    {
        m_loginState = LS_NOTLOGIN;
        if(m_sink)
        {
            write_log(m_sink, LL_INFO, "[ParserQDP] User logout successfully");
            m_sink->handleEvent(WPE_Logout, 0);
        }
    }
}

void ParserQDP::OnRspSubMarketData(CQdFtdcSpecificInstrumentField *pSpecificInstrument, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if(!IsErrorRspInfo(pRspInfo))
    {
        if(pSpecificInstrument && m_sink)
        {
            write_log(m_sink, LL_INFO, "[ParserQDP] Subscribe market data successfully: {}", pSpecificInstrument->InstrumentID);
        }
    }
}

void ParserQDP::OnRspUnSubMarketData(CQdFtdcSpecificInstrumentField *pSpecificInstrument, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if(!IsErrorRspInfo(pRspInfo))
    {
        if(pSpecificInstrument && m_sink)
        {
            write_log(m_sink, LL_INFO, "[ParserQDP] Unsubscribe market data successfully: {}", pSpecificInstrument->InstrumentID);
        }
    }
}

void ParserQDP::OnRtnDepthMarketData(CQdFtdcDepthMarketDataField *pDepthMarketData)
{
    if(m_pBaseDataMgr == NULL || pDepthMarketData == NULL)
        return;

    // 处理时间
    uint32_t actDate = strtoul(pDepthMarketData->TradingDay, NULL, 10);
    uint32_t actTime = strToTime(pDepthMarketData->UpdateTime) * 1000 + pDepthMarketData->UpdateMillisec;
    uint32_t actHour = actTime / 10000000;

    if (actDate == 0)
        actDate = m_uTradingDate;

	if (actDate == m_uTradingDate && actHour >= 20)
	{
		//这样的时间是有问题,因为夜盘时发生日期不可能等于交易日
		//这就需要手动设置一下
		uint32_t curDate, curTime;
		TimeUtils::getDateTime(curDate, curTime);
		uint32_t curHour = curTime / 10000000;

		//早上启动以后,会收到昨晚12点以前收盘的行情,这个时候可能会有发生日期=交易日的情况出现
		//这笔数据直接丢掉
		if (curHour >= 3 && curHour < 9)
			return;

		actDate = curDate;

		if (actHour == 23 && curHour == 0)
		{
			//行情时间慢于系统时间
			actDate = TimeUtils::getNextDate(curDate, -1);
		}
		else if (actHour == 0 && curHour == 23)
		{
			//系统时间慢于行情时间
			actDate = TimeUtils::getNextDate(curDate, 1);
		}
	}

	
    WTSContractInfo* contract = m_pBaseDataMgr->getContract(pDepthMarketData->InstrumentID, pDepthMarketData->ExchangeID);
    if (contract == NULL)
        return;

    WTSCommodityInfo* pCommInfo = contract->getCommInfo();

    WTSTickData* tick = WTSTickData::create(pDepthMarketData->InstrumentID);
    tick->setContractInfo(contract);
    WTSTickStruct& quote = tick->getTickStruct();
    strcpy(quote.exchg, pCommInfo->getExchg());
    
    quote.action_date = actDate;
    quote.action_time = actTime;
    quote.trading_date = m_uTradingDate;
    
    // 基础行情数据
    quote.price = checkValid(pDepthMarketData->LastPrice);
    quote.open = checkValid(pDepthMarketData->OpenPrice);
    quote.high = checkValid(pDepthMarketData->HighestPrice);
    quote.low = checkValid(pDepthMarketData->LowestPrice);
    quote.total_volume = pDepthMarketData->Volume;
	if (pDepthMarketData->SettlementPrice != DBL_MAX)
		quote.settle_price = checkValid(pDepthMarketData->SettlementPrice);
	if (strcmp(quote.exchg, "CZCE") == 0)
	{
		quote.total_turnover = pDepthMarketData->Turnover * pCommInfo->getVolScale();
	}
	else
	{
		if (pDepthMarketData->Turnover != DBL_MAX)
			quote.total_turnover = pDepthMarketData->Turnover;
	}
	quote.open_interest = (uint32_t)pDepthMarketData->OpenInterest;

    quote.upper_limit = checkValid(pDepthMarketData->UpperLimitPrice);
    quote.lower_limit = checkValid(pDepthMarketData->LowerLimitPrice);

    quote.pre_close = checkValid(pDepthMarketData->PreClosePrice);
    quote.pre_settle = checkValid(pDepthMarketData->PreSettlementPrice);
    quote.pre_interest = (uint32_t)pDepthMarketData->PreOpenInterest;

    // 五档行情
    quote.ask_prices[0] = checkValid(pDepthMarketData->AskPrice1);
    quote.ask_prices[1] = checkValid(pDepthMarketData->AskPrice2);
    quote.ask_prices[2] = checkValid(pDepthMarketData->AskPrice3);
    quote.ask_prices[3] = checkValid(pDepthMarketData->AskPrice4);
    quote.ask_prices[4] = checkValid(pDepthMarketData->AskPrice5);

    quote.bid_prices[0] = checkValid(pDepthMarketData->BidPrice1);
    quote.bid_prices[1] = checkValid(pDepthMarketData->BidPrice2);
    quote.bid_prices[2] = checkValid(pDepthMarketData->BidPrice3);
    quote.bid_prices[3] = checkValid(pDepthMarketData->BidPrice4);
    quote.bid_prices[4] = checkValid(pDepthMarketData->BidPrice5);

    quote.ask_qty[0] = pDepthMarketData->AskVolume1;
    quote.ask_qty[1] = pDepthMarketData->AskVolume2;
    quote.ask_qty[2] = pDepthMarketData->AskVolume3;
    quote.ask_qty[3] = pDepthMarketData->AskVolume4;
    quote.ask_qty[4] = pDepthMarketData->AskVolume5;

    quote.bid_qty[0] = pDepthMarketData->BidVolume1;
    quote.bid_qty[1] = pDepthMarketData->BidVolume2;
    quote.bid_qty[2] = pDepthMarketData->BidVolume3;
    quote.bid_qty[3] = pDepthMarketData->BidVolume4;
    quote.bid_qty[4] = pDepthMarketData->BidVolume5;

    write_log(m_sink, LL_INFO, "[ParserQDP] code:{}, bid_price:{}, ask_price:{}",
		quote.code, quote.bid_prices[0], quote.ask_prices[0]);

    if(m_sink)
        m_sink->handleQuote(tick, 1);

    tick->release();
}

void ParserQDP::OnRtnMultiDepthMarketData(CQdFtdcDepthMarketDataField *pDepthMarketData)
{
    // 多播行情数据，处理逻辑与单播相同
    OnRtnDepthMarketData(pDepthMarketData);
}

void ParserQDP::ReqUserLogin()
{
    if(m_pUserAPI == NULL)
        return;

    CQdFtdcReqUserLoginField req;
    memset(&req, 0, sizeof(req));
    strcpy(req.BrokerID, m_strBroker.c_str());
    strcpy(req.UserID, m_strUserID.c_str());
    strcpy(req.Password, m_strPassword.c_str());
    strcpy(req.UserProductInfo, "WT");

    m_loginState = LS_LOGINING;
    int iResult = m_pUserAPI->ReqUserLogin(&req, ++m_iRequestID);
    if(iResult != 0)
    {
        m_loginState = LS_NOTLOGIN;
        if(m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Sending login request failed: {}", iResult);
    }
}

void ParserQDP::SubscribeMarketData()
{
    if(m_filterSubs.empty())
        return;

    char ** subscribe = new char*[m_filterSubs.size()];
    int nCount = 0;
    for(auto& code : m_filterSubs)
    {
        std::size_t pos = code.find('.');
        if (pos != std::string::npos)
            subscribe[nCount++] = (char*)code.c_str() + pos + 1;
        else
            subscribe[nCount++] = (char*)code.c_str();
        write_log(m_sink, LL_INFO, "[ParserQDP] code:{} ready to sub", code);
    }

    if(m_pUserAPI && nCount > 0)
    {
        int iResult = m_pUserAPI->SubMarketData(subscribe, nCount);
        if(iResult != 0)
        {
            if(m_sink)
                write_log(m_sink, LL_ERROR, "[ParserQDP] Sending md subscribe request failed: {}", iResult);
        }
        else
        {
            if(m_sink)
                write_log(m_sink, LL_INFO, "[ParserQDP] Market data of {} contracts subscribed in total", nCount);
        }
    }
    delete[] subscribe;
    m_filterSubs.clear();
}

bool ParserQDP::IsErrorRspInfo(CQdFtdcRspInfoField *pRspInfo)
{
    if(pRspInfo && pRspInfo->ErrorID != 0)
    {
        if(m_sink)
            write_log(m_sink, LL_ERROR, "[ParserQDP] Error response: ErrorID={}, ErrorMsg={}", 
                     pRspInfo->ErrorID, pRspInfo->ErrorMsg);
        return true;
    }
    return false;
}