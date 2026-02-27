/////////////////////////////////////////////////////////////////////////
///@system QuantDo Platform
///@company 上海量投网络科技有限公司
///@file QdFtdcUserApiStruct.h
///@brief 定义了客户端接口使用的业务数据结构
///@history 
/////////////////////////////////////////////////////////////////////////

#if !defined(Qd_FTDCSTRUCT_H)
#define Qd_FTDCSTRUCT_H

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "QdFtdcUserApiDataType.h"

///响应信息
struct CQdFtdcRspInfoField
{
	///错误代码
	TQdFtdcErrorIDType	ErrorID;
	///错误信息
	TQdFtdcErrorMsgType	ErrorMsg;
};

///Qdp响应信息
struct CQdFtdcQdpRspInfoField
{
	///错误代码
	TQdFtdcErrorIDType	ErrorID;
	///错误信息
	TQdFtdcQdpErrorMsgType	ErrorMsg;
};

///系统用户登录请求
struct CQdFtdcReqUserLoginField
{
	///交易日
	TQdFtdcDateType	TradingDay;
	///交易用户代码
	TQdFtdcUserIDType	UserID;
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///密码
	TQdFtdcPasswordType	Password;
	///用户端产品信息
	TQdFtdcProductInfoType	UserProductInfo;
	///接口端产品信息
	TQdFtdcProductInfoType	InterfaceProductInfo;
	///协议信息
	TQdFtdcProtocolInfoType	ProtocolInfo;
	///IP地址
	TQdFtdcIPAddressType	IPAddress;
	///Mac地址
	TQdFtdcMacAddressType	MacAddress;
	///数据中心代码
	TQdFtdcDataCenterIDType	DataCenterID;
	///动态密码
	TQdFtdcPasswordType	OneTimePassword;
	///终端IP地址
	TQdFtdcIPAddressType	ClientIPAddress;
	///授权编码
	TQdFtdcAuthCodeType	AuthCode;
};

///Qdp系统用户登录请求
struct CQdFtdcQdpReqUserLoginField
{
	///交易日
	TQdFtdcDateType	TradingDay;
	///交易用户代码
	TQdFtdcQdpUserIDType	UserID;
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///密码
	TQdFtdcPasswordType	Password;
	///用户端产品信息
	TQdFtdcQdpProductInfoType	UserProductInfo;
	///接口端产品信息
	TQdFtdcQdpProductInfoType	InterfaceProductInfo;
	///协议信息
	TQdFtdcProtocolInfoType	ProtocolInfo;
	///IP地址
	TQdFtdcIPAddressType	IPAddress;
	///Mac地址
	TQdFtdcMacAddressType	MacAddress;
	///数据中心代码
	TQdFtdcDataCenterIDType	DataCenterID;
	///动态密码
	TQdFtdcPasswordType	OneTimePassword;
	///终端IP地址
	TQdFtdcIPAddressType	ClientIPAddress;
};

///系统用户登录应答
struct CQdFtdcRspUserLoginField
{
	///交易日
	TQdFtdcDateType	TradingDay;
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///交易用户代码
	TQdFtdcUserIDType	UserID;
	///登录成功时间
	TQdFtdcTimeType	LoginTime;
	///用户最大本地报单号
	TQdFtdcUserOrderLocalIDType	MaxOrderLocalID;
	///交易系统名称
	TQdFtdcTradingSystemNameType	TradingSystemName;
	///数据中心代码
	TQdFtdcDataCenterIDType	DataCenterID;
	///会员私有流当前长度
	TQdFtdcSequenceNoType	PrivateFlowSize;
	///交易员私有流当前长度
	TQdFtdcSequenceNoType	UserFlowSize;
	///登录附加信息
	TQdFtdcLoginInfoType	LoginInfo;
	///会话编号
	TQdFtdcSessionIDType	SessionID;
	///前置编号
	TQdFtdcFrontIDType	FrontID;
};

///Qdp系统用户登录应答
struct CQdFtdcQdpRspUserLoginField
{
	///交易日
	TQdFtdcDateType	TradingDay;
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///交易用户代码
	TQdFtdcQdpUserIDType	UserID;
	///登录成功时间
	TQdFtdcTimeType	LoginTime;
	///用户最大本地报单号
	TQdFtdcUserOrderLocalIDType	MaxOrderLocalID;
	///交易系统名称
	TQdFtdcTradingSystemNameType	TradingSystemName;
	///数据中心代码
	TQdFtdcDataCenterIDType	DataCenterID;
	///会员私有流当前长度
	TQdFtdcSequenceNoType	PrivateFlowSize;
	///交易员私有流当前长度
	TQdFtdcSequenceNoType	UserFlowSize;
	///会话编号
	TQdFtdcSessionIDType	SessionID;
	///前置编号
	TQdFtdcFrontIDType	FrontID;
};

///用户登出请求
struct CQdFtdcReqUserLogoutField
{
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///交易用户代码
	TQdFtdcUserIDType	UserID;
};

///用户登出请求
struct CQdFtdcRspUserLogoutField
{
	///经纪公司编号
	TQdFtdcBrokerIDType	BrokerID;
	///交易用户代码
	TQdFtdcUserIDType	UserID;
};

///行情基础属性
struct CQdFtdcMarketDataBaseField
{
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
};

///Qdp行情基础属性
struct CQdFtdcQdpMarketDataBaseField
{
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///结算组代码
	TQdFtdcSettlementGroupIDType	SettlementGroupID;
	///结算编号
	TQdFtdcSettlementIDType	SettlementID;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
};

///行情静态属性
struct CQdFtdcMarketDataStaticField
{
	///今开盘
	TQdFtdcPriceType	OpenPrice;
	///最高价
	TQdFtdcPriceType	HighestPrice;
	///最低价
	TQdFtdcPriceType	LowestPrice;
	///今收盘
	TQdFtdcPriceType	ClosePrice;
	///涨停板价
	TQdFtdcPriceType	UpperLimitPrice;
	///跌停板价
	TQdFtdcPriceType	LowerLimitPrice;
	///今结算
	TQdFtdcPriceType	SettlementPrice;
	///今虚实度
	TQdFtdcRatioType	CurrDelta;
};

///行情最新成交属性
struct CQdFtdcMarketDataLastMatchField
{
	///最新价
	TQdFtdcPriceType	LastPrice;
	///数量
	TQdFtdcVolumeType	Volume;
	///成交金额
	TQdFtdcMoneyType	Turnover;
	///持仓量
	TQdFtdcLargeVolumeType	OpenInterest;
};

///行情最优价属性
struct CQdFtdcMarketDataBestPriceField
{
	///申买价一
	TQdFtdcPriceType	BidPrice1;
	///申买量一
	TQdFtdcVolumeType	BidVolume1;
	///申卖价一
	TQdFtdcPriceType	AskPrice1;
	///申卖量一
	TQdFtdcVolumeType	AskVolume1;
};

///行情申买二、三属性
struct CQdFtdcMarketDataBid23Field
{
	///申买价二
	TQdFtdcPriceType	BidPrice2;
	///申买量二
	TQdFtdcVolumeType	BidVolume2;
	///申买价三
	TQdFtdcPriceType	BidPrice3;
	///申买量三
	TQdFtdcVolumeType	BidVolume3;
};

///行情申卖二、三属性
struct CQdFtdcMarketDataAsk23Field
{
	///申卖价二
	TQdFtdcPriceType	AskPrice2;
	///申卖量二
	TQdFtdcVolumeType	AskVolume2;
	///申卖价三
	TQdFtdcPriceType	AskPrice3;
	///申卖量三
	TQdFtdcVolumeType	AskVolume3;
};

///行情申买四、五属性
struct CQdFtdcMarketDataBid45Field
{
	///申买价四
	TQdFtdcPriceType	BidPrice4;
	///申买量四
	TQdFtdcVolumeType	BidVolume4;
	///申买价五
	TQdFtdcPriceType	BidPrice5;
	///申买量五
	TQdFtdcVolumeType	BidVolume5;
};

///行情申卖四、五属性
struct CQdFtdcMarketDataAsk45Field
{
	///申卖价四
	TQdFtdcPriceType	AskPrice4;
	///申卖量四
	TQdFtdcVolumeType	AskVolume4;
	///申卖价五
	TQdFtdcPriceType	AskPrice5;
	///申卖量五
	TQdFtdcVolumeType	AskVolume5;
};

///行情更新时间属性
struct CQdFtdcMarketDataUpdateTimeField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
};

///深度行情
struct CQdFtdcDepthMarketDataField
{
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
	///今开盘
	TQdFtdcPriceType	OpenPrice;
	///最高价
	TQdFtdcPriceType	HighestPrice;
	///最低价
	TQdFtdcPriceType	LowestPrice;
	///今收盘
	TQdFtdcPriceType	ClosePrice;
	///涨停板价
	TQdFtdcPriceType	UpperLimitPrice;
	///跌停板价
	TQdFtdcPriceType	LowerLimitPrice;
	///今结算
	TQdFtdcPriceType	SettlementPrice;
	///今虚实度
	TQdFtdcRatioType	CurrDelta;
	///最新价
	TQdFtdcPriceType	LastPrice;
	///数量
	TQdFtdcVolumeType	Volume;
	///成交金额
	TQdFtdcMoneyType	Turnover;
	///持仓量
	TQdFtdcLargeVolumeType	OpenInterest;
	///申买价一
	TQdFtdcPriceType	BidPrice1;
	///申买量一
	TQdFtdcVolumeType	BidVolume1;
	///申卖价一
	TQdFtdcPriceType	AskPrice1;
	///申卖量一
	TQdFtdcVolumeType	AskVolume1;
	///申买价二
	TQdFtdcPriceType	BidPrice2;
	///申买量二
	TQdFtdcVolumeType	BidVolume2;
	///申买价三
	TQdFtdcPriceType	BidPrice3;
	///申买量三
	TQdFtdcVolumeType	BidVolume3;
	///申卖价二
	TQdFtdcPriceType	AskPrice2;
	///申卖量二
	TQdFtdcVolumeType	AskVolume2;
	///申卖价三
	TQdFtdcPriceType	AskPrice3;
	///申卖量三
	TQdFtdcVolumeType	AskVolume3;
	///申买价四
	TQdFtdcPriceType	BidPrice4;
	///申买量四
	TQdFtdcVolumeType	BidVolume4;
	///申买价五
	TQdFtdcPriceType	BidPrice5;
	///申买量五
	TQdFtdcVolumeType	BidVolume5;
	///申卖价四
	TQdFtdcPriceType	AskPrice4;
	///申卖量四
	TQdFtdcVolumeType	AskVolume4;
	///申卖价五
	TQdFtdcPriceType	AskPrice5;
	///申卖量五
	TQdFtdcVolumeType	AskVolume5;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///总卖出数量
	TQdFtdcVolumeType	VolumeAskLot;
	///总买入数量
	TQdFtdcVolumeType	VolumeBidLot;
	///合约状态
	TQdFtdcInstrumentStatusType	InstrumentStatus;
	///当前日期
	TQdFtdcTradingDayType	CalendarDate;
	///组播增量序号
	TQdFtdcPacketNoType	PacketNo;
	///合约序号
	TQdFtdcNumberType	InstrumentNo;
	///数据中心代码
	TQdFtdcDataCenterIDType	DataCenterID;
};

///深度行情
struct CQdFtdcQdpDepthMarketDataField
{
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///结算组代码
	TQdFtdcSettlementGroupIDType	SettlementGroupID;
	///结算编号
	TQdFtdcSettlementIDType	SettlementID;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
	///今开盘
	TQdFtdcPriceType	OpenPrice;
	///最高价
	TQdFtdcPriceType	HighestPrice;
	///最低价
	TQdFtdcPriceType	LowestPrice;
	///今收盘
	TQdFtdcPriceType	ClosePrice;
	///涨停板价
	TQdFtdcPriceType	UpperLimitPrice;
	///跌停板价
	TQdFtdcPriceType	LowerLimitPrice;
	///今结算
	TQdFtdcPriceType	SettlementPrice;
	///今虚实度
	TQdFtdcRatioType	CurrDelta;
	///最新价
	TQdFtdcPriceType	LastPrice;
	///数量
	TQdFtdcVolumeType	Volume;
	///成交金额
	TQdFtdcMoneyType	Turnover;
	///持仓量
	TQdFtdcLargeVolumeType	OpenInterest;
	///申买价一
	TQdFtdcPriceType	BidPrice1;
	///申买量一
	TQdFtdcVolumeType	BidVolume1;
	///申卖价一
	TQdFtdcPriceType	AskPrice1;
	///申卖量一
	TQdFtdcVolumeType	AskVolume1;
	///申买价二
	TQdFtdcPriceType	BidPrice2;
	///申买量二
	TQdFtdcVolumeType	BidVolume2;
	///申买价三
	TQdFtdcPriceType	BidPrice3;
	///申买量三
	TQdFtdcVolumeType	BidVolume3;
	///申卖价二
	TQdFtdcPriceType	AskPrice2;
	///申卖量二
	TQdFtdcVolumeType	AskVolume2;
	///申卖价三
	TQdFtdcPriceType	AskPrice3;
	///申卖量三
	TQdFtdcVolumeType	AskVolume3;
	///申买价四
	TQdFtdcPriceType	BidPrice4;
	///申买量四
	TQdFtdcVolumeType	BidVolume4;
	///申买价五
	TQdFtdcPriceType	BidPrice5;
	///申买量五
	TQdFtdcVolumeType	BidVolume5;
	///申卖价四
	TQdFtdcPriceType	AskPrice4;
	///申卖量四
	TQdFtdcVolumeType	AskVolume4;
	///申卖价五
	TQdFtdcPriceType	AskPrice5;
	///申卖量五
	TQdFtdcVolumeType	AskVolume5;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
};

///订阅合约的相关信息
struct CQdFtdcSpecificInstrumentField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
};

///交易所代码
struct CQdFtdcMarketDataExchangeIDField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
};

///Qdp交易所代码
struct CQdFtdcQdpMarketDataExchangeIDField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
};

///共享内存行情查询
struct CQdFtdcShmDepthMarketDataField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///IP地址
	TQdFtdcIPAddressType	IPAddress;
	///订阅号
	TQdFtdcTopicIDType	TopicID;
};

///TOPIC查询
struct CQdFtdcTopicSearchField
{
	///订阅号
	TQdFtdcTopicIDType	TopicID;
};

///分价行情
struct CQdFtdcMBLMarketDataField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///买卖方向
	TQdFtdcDirectionType	Direction;
	///价格
	TQdFtdcPriceType	Price;
	///数量
	TQdFtdcVolumeType	Volume;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
};

///套利合约买卖总量
struct CQdFtdcMarketDataCMBVolumeField
{
	///总卖出数量
	TQdFtdcVolumeType	VolumeAskLot;
	///总买入数量
	TQdFtdcVolumeType	VolumeBidLot;
};

///合约状态
struct CQdFtdcQmdInstrumentStateField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///合约交易状态
	TQdFtdcInstrumentStatusType	InstrumentStatus;
};

///行情合约状态
struct CQdFtdcMarketDataInstrumentStatusField
{
	///合约状态
	TQdFtdcInstrumentStatusType	InstrumentStatus;
};

///信息分发
struct CQdFtdcDisseminationField
{
	///序列系列号
	TQdFtdcSequenceSeriesType	SequenceSeries;
	///序列号
	TQdFtdcSequenceNoType	SequenceNo;
};

///Qdp信息分发
struct CQdFtdcQdpDisseminationField
{
	///序列系列号
	TQdFtdcSequenceSeriesType	SequenceSeries;
	///序列号
	TQdFtdcSequenceNoType	SequenceNo;
};

///行情查询
struct CQdFtdcQryMarketDataField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///订阅号
	TQdFtdcTopicIDType	TopicID;
};

///行情应答
struct CQdFtdcRspMarketDataField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
	///今开盘
	TQdFtdcPriceType	OpenPrice;
	///最高价
	TQdFtdcPriceType	HighestPrice;
	///最低价
	TQdFtdcPriceType	LowestPrice;
	///今收盘
	TQdFtdcPriceType	ClosePrice;
	///涨停板价
	TQdFtdcPriceType	UpperLimitPrice;
	///跌停板价
	TQdFtdcPriceType	LowerLimitPrice;
	///今结算
	TQdFtdcPriceType	SettlementPrice;
	///今虚实度
	TQdFtdcRatioType	CurrDelta;
	///最新价
	TQdFtdcPriceType	LastPrice;
	///数量
	TQdFtdcVolumeType	Volume;
	///成交金额
	TQdFtdcMoneyType	Turnover;
	///持仓量
	TQdFtdcLargeVolumeType	OpenInterest;
	///申买价一
	TQdFtdcPriceType	BidPrice1;
	///申买量一
	TQdFtdcVolumeType	BidVolume1;
	///申卖价一
	TQdFtdcPriceType	AskPrice1;
	///申卖量一
	TQdFtdcVolumeType	AskVolume1;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
	///合约状态
	TQdFtdcInstrumentStatusType	InstrumentStatus;
};

///MDTen最优买属性
struct CQdFtdcMDTenBestBuyOrderField
{
	///最优买价格
	TQdFtdcPriceType	BestBuyOrderPrice;
	///申买量一
	TQdFtdcVolumeType	BestBuyOrderQtyOne;
	///申买量二
	TQdFtdcVolumeType	BestBuyOrderQtyTwo;
	///申买量三
	TQdFtdcVolumeType	BestBuyOrderQtyThree;
	///申买量四
	TQdFtdcVolumeType	BestBuyOrderQtyFour;
	///申买量五
	TQdFtdcVolumeType	BestBuyOrderQtyFive;
	///申买量六
	TQdFtdcVolumeType	BestBuyOrderQtySix;
	///申买量七
	TQdFtdcVolumeType	BestBuyOrderQtySeven;
	///申买量八
	TQdFtdcVolumeType	BestBuyOrderQtyEight;
	///申买量九
	TQdFtdcVolumeType	BestBuyOrderQtyNine;
	///申买量十
	TQdFtdcVolumeType	BestBuyOrderQtyTen;
};

///MDTen最优卖属性
struct CQdFtdcMDTenBestSellOrderField
{
	///最优卖价格
	TQdFtdcPriceType	BestSellOrderPrice;
	///申卖量一
	TQdFtdcVolumeType	BestSellOrderQtyOne;
	///申卖量二
	TQdFtdcVolumeType	BestSellOrderQtyTwo;
	///申卖量三
	TQdFtdcVolumeType	BestSellOrderQtyThree;
	///申卖量四
	TQdFtdcVolumeType	BestSellOrderQtyFour;
	///申卖量五
	TQdFtdcVolumeType	BestSellOrderQtyFive;
	///申卖量六
	TQdFtdcVolumeType	BestSellOrderQtySix;
	///申卖量七
	TQdFtdcVolumeType	BestSellOrderQtySeven;
	///申卖量八
	TQdFtdcVolumeType	BestSellOrderQtyEight;
	///申卖量九
	TQdFtdcVolumeType	BestSellOrderQtyNine;
	///申卖量十
	TQdFtdcVolumeType	BestSellOrderQtyTen;
};

///MDTen深度行情
struct CQdFtdcMDTenDepthMarketDataField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
	///最优买价格
	TQdFtdcPriceType	BestBuyOrderPrice;
	///申买量一
	TQdFtdcVolumeType	BestBuyOrderQtyOne;
	///申买量二
	TQdFtdcVolumeType	BestBuyOrderQtyTwo;
	///申买量三
	TQdFtdcVolumeType	BestBuyOrderQtyThree;
	///申买量四
	TQdFtdcVolumeType	BestBuyOrderQtyFour;
	///申买量五
	TQdFtdcVolumeType	BestBuyOrderQtyFive;
	///申买量六
	TQdFtdcVolumeType	BestBuyOrderQtySix;
	///申买量七
	TQdFtdcVolumeType	BestBuyOrderQtySeven;
	///申买量八
	TQdFtdcVolumeType	BestBuyOrderQtyEight;
	///申买量九
	TQdFtdcVolumeType	BestBuyOrderQtyNine;
	///申买量十
	TQdFtdcVolumeType	BestBuyOrderQtyTen;
	///最优卖价格
	TQdFtdcPriceType	BestSellOrderPrice;
	///申卖量一
	TQdFtdcVolumeType	BestSellOrderQtyOne;
	///申卖量二
	TQdFtdcVolumeType	BestSellOrderQtyTwo;
	///申卖量三
	TQdFtdcVolumeType	BestSellOrderQtyThree;
	///申卖量四
	TQdFtdcVolumeType	BestSellOrderQtyFour;
	///申卖量五
	TQdFtdcVolumeType	BestSellOrderQtyFive;
	///申卖量六
	TQdFtdcVolumeType	BestSellOrderQtySix;
	///申卖量七
	TQdFtdcVolumeType	BestSellOrderQtySeven;
	///申卖量八
	TQdFtdcVolumeType	BestSellOrderQtyEight;
	///申卖量九
	TQdFtdcVolumeType	BestSellOrderQtyNine;
	///申卖量十
	TQdFtdcVolumeType	BestSellOrderQtyTen;
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
};

///期权静态属性
struct CQdFtdcOptionStaticField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///用户
	TQdFtdcUserIDType	UserID;
	///客户端设置波动率
	TQdFtdcManalSetVolType	IsManalSetVol;
};

///期权最新价
struct CQdFtdcOptionLastpriceField
{
	///标的最新价
	TQdFtdcPriceType	UnderlyingLastPrice;
	///最新价
	TQdFtdcPriceType	LastPrice;
	///理论波动率
	TQdFtdcPriceType	TheoryVol;
};

///期权买一价
struct CQdFtdcOptionBidpriceField
{
	///标的买1价
	TQdFtdcPriceType	UnderlyingBidPrice1;
	///买1价
	TQdFtdcPriceType	BidPrice1;
};

///期权买一价
struct CQdFtdcOptionAskpriceField
{
	///标的卖1价
	TQdFtdcPriceType	UnderlyingAskPrice1;
	///卖1价
	TQdFtdcPriceType	AskPrice1;
};

///期权希腊值
struct CQdFtdcOptionGreeksField
{
	///剩余天数
	TQdFtdcPriceType	RemainDay;
	///卖1价波动率
	TQdFtdcPriceType	AskVol;
	///买1价波动率
	TQdFtdcPriceType	BidVol;
	///最新价波动率
	TQdFtdcPriceType	LastVol;
	///中间价波动率
	TQdFtdcPriceType	MidVol;
	///Delta
	TQdFtdcPriceType	Delta;
	///Gamma
	TQdFtdcPriceType	Gamma;
	///Vega
	TQdFtdcPriceType	Vega;
	///Theta
	TQdFtdcPriceType	Theta;
	///Rho
	TQdFtdcPriceType	Rho;
};

///期权行情
struct CQdFtdcOptionIndexDataField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///最后修改时间
	TQdFtdcTimeType	UpdateTime;
	///最后修改毫秒
	TQdFtdcMillisecType	UpdateMillisec;
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///用户
	TQdFtdcUserIDType	UserID;
	///客户端设置波动率
	TQdFtdcManalSetVolType	IsManalSetVol;
	///标的最新价
	TQdFtdcPriceType	UnderlyingLastPrice;
	///最新价
	TQdFtdcPriceType	LastPrice;
	///理论波动率
	TQdFtdcPriceType	TheoryVol;
	///标的买1价
	TQdFtdcPriceType	UnderlyingBidPrice1;
	///买1价
	TQdFtdcPriceType	BidPrice1;
	///标的卖1价
	TQdFtdcPriceType	UnderlyingAskPrice1;
	///卖1价
	TQdFtdcPriceType	AskPrice1;
	///剩余天数
	TQdFtdcPriceType	RemainDay;
	///卖1价波动率
	TQdFtdcPriceType	AskVol;
	///买1价波动率
	TQdFtdcPriceType	BidVol;
	///最新价波动率
	TQdFtdcPriceType	LastVol;
	///中间价波动率
	TQdFtdcPriceType	MidVol;
	///Delta
	TQdFtdcPriceType	Delta;
	///Gamma
	TQdFtdcPriceType	Gamma;
	///Vega
	TQdFtdcPriceType	Vega;
	///Theta
	TQdFtdcPriceType	Theta;
	///Rho
	TQdFtdcPriceType	Rho;
};

///当前日期
struct CQdFtdcMarketDataActionDayField
{
	///当前日期
	TQdFtdcTradingDayType	CalendarDate;
};

///查询组播信息
struct CQdFtdcQryShfeMultiInfoField
{
	///交易所代码
	TQdFtdcExchangeIDType	ExchangeID;
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
};

///上期组播深度信息
struct CQdFtdcShfeMultiLevelField
{
	///订阅号
	TQdFtdcTopicIDType	TopicID;
	///深度
	TQdFtdcNumberType	Level;
};

///上期组播解析参数
struct CQdFtdcShfeMultiParametersField
{
	///合约代码
	TQdFtdcInstrumentIDType	InstrumentID;
	///交易日
	TQdFtdcTradingDayType	TradingDay;
	///昨结算
	TQdFtdcPriceType	PreSettlementPrice;
	///昨收盘
	TQdFtdcPriceType	PreClosePrice;
	///昨持仓量
	TQdFtdcLargeVolumeType	PreOpenInterest;
	///昨虚实度
	TQdFtdcRatioType	PreDelta;
	///今开盘
	TQdFtdcPriceType	OpenPrice;
	///最高价
	TQdFtdcPriceType	HighestPrice;
	///最低价
	TQdFtdcPriceType	LowestPrice;
	///今收盘
	TQdFtdcPriceType	ClosePrice;
	///涨停板价
	TQdFtdcPriceType	UpperLimitPrice;
	///跌停板价
	TQdFtdcPriceType	LowerLimitPrice;
	///今结算
	TQdFtdcPriceType	SettlementPrice;
	///今虚实度
	TQdFtdcRatioType	CurrDelta;
	///品种
	TQdFtdcBoolType	ProductClass;
	///执行价格
	TQdFtdcPriceType	StrikePrice;
	///合约乘数
	TQdFtdcNumberType	VolumeMultiple;
	///最小变动价格
	TQdFtdcPriceType	PriceTick;
	///编解码价格
	TQdFtdcPriceType	CodecPrice;
	///合约序号
	TQdFtdcNumberType	InstrumentNo;
};



#endif
