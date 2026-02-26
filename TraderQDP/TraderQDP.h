/*!
 * \file TraderQDP.h
 * \project	WonderTrader
 *
 * \author YourName
 * \date 2024/01/01
 * 
 * \brief QDP柜台交易通道对接模块
 */
#pragma once

#include <string>
#include <queue>
#include <stdint.h>

#include "../Includes/WTSTypes.h"
#include "../Includes/ITraderApi.h"
#include "../Includes/WTSCollection.hpp"

#include "../API/QDP7.0.0/QdpFtdcTraderApi.h"

#include "../Share/StdUtils.hpp"
#include "../Share/DLLHelper.hpp"
#include "../Share/WtKVCache.hpp"

USING_NS_WTP;

class TraderQDP : public ITraderApi, public CQdpFtdcTraderSpi
{
public:
    TraderQDP();
    virtual ~TraderQDP();

public:
    typedef enum
    {
        WS_NOTLOGIN,        //未登录
        WS_LOGINING,        //正在登录
        WS_LOGINED,         //已登录
        WS_LOGINFAILED,     //登录失败
        WS_ALLREADY         //全部就绪
    } WrapperState;

private:
    int authenticate();
    int doLogin();

    //////////////////////////////////////////////////////////////////////////
    //ITraderApi接口
public:
    virtual bool init(WTSVariant* params) override;
    virtual void release() override;
    virtual void registerSpi(ITraderSpi *listener) override;
    virtual bool makeEntrustID(char* buffer, int length) override;
    virtual void connect() override;
    virtual void disconnect() override;
    virtual bool isConnected() override;
    virtual int login(const char* user, const char* pass, const char* productInfo) override;
    virtual int logout() override;
    virtual int orderInsert(WTSEntrust* entrust) override;
    virtual int orderAction(WTSEntrustAction* action) override;
    virtual int queryAccount() override;
    virtual int queryPositions() override;
    virtual int queryOrders() override;
    virtual int queryTrades() override;

    //////////////////////////////////////////////////////////////////////////
    //QDP交易接口回调
public:
    virtual void OnFrontConnected() override;
    virtual void OnFrontDisconnected(int nReason) override;
    virtual void OnHeartBeatWarning(int nTimeLapse) override;
    
    virtual void OnRspAuthenticate(CQdpFtdcRtnAuthenticateField *pRtnAuthenticate, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspUserLogin(CQdpFtdcRspUserLoginField *pRspUserLogin, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspUserLogout(CQdpFtdcRspUserLogoutField *pRspUserLogout, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    
    virtual void OnRspOrderInsert(CQdpFtdcRspInputOrderField *pRspInputOrder, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspOrderAction(CQdpFtdcOrderActionField *pOrderAction, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    
    virtual void OnRspQryOrder(CQdpFtdcOrderField *pOrder, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspQryTrade(CQdpFtdcTradeField *pTrade, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspQryInvestorPosition(CQdpFtdcRspInvestorPositionField *pRspInvestorPosition, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    virtual void OnRspQryInvestorAccount(CQdpFtdcRspInvestorAccountField *pRspInvestorAccount, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    
    virtual void OnRtnOrder(CQdpFtdcOrderField *pOrder) override;
    virtual void OnRtnTrade(CQdpFtdcTradeField *pTrade) override;
    
    virtual void OnErrRtnOrderInsert(CQdpFtdcRspInputOrderField *pRspInputOrder, CQdpFtdcRspInfoField *pRspInfo) override;
    virtual void OnErrRtnOrderAction(CQdpFtdcOrderActionField *pOrderAction, CQdpFtdcRspInfoField *pRspInfo) override;
    
    virtual void OnRtnInstrumentStatus(CQdpFtdcInstrumentStatusField *pInstrumentStatus) override;

	///合约查询应答
	virtual void OnRspQryInstrument(CQdpFtdcRspInstrumentField *pRspInstrument, CQdpFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;


protected:
    bool IsErrorRspInfo(CQdpFtdcRspInfoField *pRspInfo);
    
    // 类型转换函数
    char wrapPriceType(WTSPriceType priceType);
    char wrapDirectionType(WTSDirectionType dirType, WTSOffsetType offType);
    char wrapOffsetType(WTSOffsetType offType);
    char wrapTimeCondition(WTSTimeCondition timeCond);
    
    WTSDirectionType wrapDirectionType(char dirType, char offType);
    WTSOffsetType wrapOffsetType(char offType);
    WTSPriceType wrapPriceType(char priceType);
    WTSTimeCondition wrapTimeCondition(char timeCond);
    WTSOrderState wrapOrderState(char orderState);
    
    // 数据转换函数
    WTSOrderInfo* makeOrderInfo(CQdpFtdcOrderField* orderField);
    WTSEntrust* makeEntrust(CQdpFtdcRspInputOrderField *entrustField);
    WTSError* makeError(CQdpFtdcRspInfoField* rspInfo);
    WTSTradeInfo* makeTradeRecord(CQdpFtdcTradeField *tradeField);
    WTSAccountInfo* makeAccountInfo(CQdpFtdcRspInvestorAccountField* accountField);
    WTSPositionItem* makePositionInfo(CQdpFtdcRspInvestorPositionField* positionField);
    
    void generateEntrustID(char* buffer, uint32_t sessionid, uint32_t orderRef);
    bool extractEntrustID(const char* entrustid, uint32_t &sessionid, uint32_t &orderRef);
    
    uint32_t genRequestID();

protected:
    std::string     m_strBroker;
    std::string     m_strFront;
    std::string     m_strUser;
    std::string     m_strPass;
    std::string     m_strAppID;
    std::string     m_strAuthCode;
    std::string     m_strFlowDir;
    std::string     m_strProdInfo;
    
    bool            m_bQuickStart;
    
    ITraderSpi*     m_sink;
    uint32_t        m_lDate;
    uint32_t        m_sessionID;
    std::atomic<uint32_t>  m_orderRef;
    
    WrapperState            m_wrapperState;
    CQdpFtdcTraderApi*      m_pUserAPI;
    std::atomic<uint32_t>   m_iRequestID;
    
    typedef WTSHashMap<std::string> PositionMap;
    PositionMap*        m_mapPosition;
    WTSArray*           m_ayTrades;
    WTSArray*           m_ayOrders;
    WTSArray*           m_ayFunds;
    
    IBaseDataMgr*       m_bdMgr;
    
    typedef std::queue<CommonExecuter>   QueryQue;
    QueryQue                m_queQuery;
    bool                    m_bInQuery;
    StdUniqueMutex          m_mtxQuery;
	uint64_t				m_uLastQryTime;
	uint64_t				m_lastQryTime;

    bool                    m_bStopped;
    StdThreadPtr            m_thrdWorker;
    
    std::string     m_strModule;
    DllHandle       m_hInstQDP;
    typedef CQdpFtdcTraderApi* (*QDPCreator)(const char *);
    QDPCreator      m_funcCreator;

	std::unordered_map<std::string, int>		m_mapInstrumentIDtoNum;
    
    // 委托单标记缓存器
    WtKVCache       m_eidCache;
    // 订单标记缓存器  
    WtKVCache       m_oidCache;
};