/*!
 * \file ParserQDP.h
 * \project	WonderTrader
 *
 * \author Wesley
 * \date 2024/01/15
 * 
 * \brief QDP柜台行情解析器
 */
#pragma once
#include "../Includes/IParserApi.h"
#include "../Share/DLLHelper.hpp"
#include "../API/QDP7.0.0/QdFtdcMdApi.h"
#include <map>

NS_WTP_BEGIN
class WTSTickData;
NS_WTP_END

USING_NS_WTP;

class ParserQDP : public IParserApi, public CQdFtdcMduserSpi
{
public:
    ParserQDP();
    virtual ~ParserQDP();

public:
    enum LoginStatus
    {
        LS_NOTLOGIN,
        LS_LOGINING,
        LS_LOGINED
    };

// IParserApi 接口
public:
    virtual bool init(WTSVariant* config) override;

    virtual void release() override;

    virtual bool connect() override;

    virtual bool disconnect() override;

    virtual bool isConnected() override;

    virtual void subscribe(const CodeSet &vecSymbols) override;

    virtual void unsubscribe(const CodeSet &vecSymbols) override;

    virtual void registerSpi(IParserSpi* listener) override;

// CQdFtdcMduserSpi 接口
public:
	virtual void OnFrontConnected() override;

	virtual void OnFrontDisconnected(int nReason) override;

	virtual void OnHeartBeatWarning(int nTimeLapse) override;

	virtual void OnRspError(CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;

	virtual void OnRspUserLogin(CQdFtdcRspUserLoginField *pRspUserLogin, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;

	virtual void OnRspUserLogout(CQdFtdcRspUserLogoutField *pRspUserLogout, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;

	virtual void OnRspSubMarketData(CQdFtdcSpecificInstrumentField *pSpecificInstrument, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;

	virtual void OnRspUnSubMarketData(CQdFtdcSpecificInstrumentField *pSpecificInstrument, CQdFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;

	virtual void OnRtnDepthMarketData(CQdFtdcDepthMarketDataField *pDepthMarketData) override;

	virtual void OnRtnMultiDepthMarketData(CQdFtdcDepthMarketDataField *pDepthMarketData) override;

private:
    /// 发送登录请求
    void ReqUserLogin();
    /// 订阅行情
    void SubscribeMarketData();
    /// 检查错误信息
    bool IsErrorRspInfo(CQdFtdcRspInfoField *pRspInfo);
    /// 时间字符串转换
    uint32_t strToTime(const char* strTime);
    /// 检查数据有效性
    inline double checkValid(double val);

private:
    uint32_t            m_uTradingDate;
    LoginStatus         m_loginState;
    CQdFtdcMduserApi*   m_pUserAPI;

    std::string         m_strFrontAddr;
    std::string         m_strBroker;
    std::string         m_strUserID;
    std::string         m_strPassword;
    std::string         m_strFlowDir;

    CodeSet             m_filterSubs;

    int                 m_iRequestID;

    IParserSpi*         m_sink;
    IBaseDataMgr*       m_pBaseDataMgr;

    DllHandle           m_hInstQDP;
    typedef CQdFtdcMduserApi* (*QDPCreator)(const char*);
    QDPCreator          m_funcCreator;
};

// 导出函数
extern "C"
{
    EXPORT_FLAG IParserApi* createParser();
    EXPORT_FLAG void deleteParser(IParserApi* &parser);
};