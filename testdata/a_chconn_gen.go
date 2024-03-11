package model

import (
	column "github.com/vahid-sohrabloo/chconn/v3/column"
	types "github.com/vahid-sohrabloo/chconn/v3/types"
)

// suppress unused package warning
var (
	_ types.Date32
)

type chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata struct {
	*column.Tuple
	EntityIDColumn                      *column.StringBase[string]
	EventColumn                         *column.Base[ModelEvent]
	SourceColumn                        *column.Base[ModelSource]
	RevenueColumn                       *column.Nullable[int64]
	ClientIDColumn                      *column.Base[uint64]
	SessionIDColumn                     *column.Base[uint64]
	PageViewIDColumn                    *column.Base[uint64]
	ImpressionIDColumn                  *column.Base[uint64]
	AdSlotIDColumn                      *column.StringBase[string]
	AdSizeColumn                        *column.StringBase[string]
	AdFloorColumn                       *column.Nullable[uint64]
	AdFloorGroupColumn                  *column.StringBase[string]
	AdFloorStatusColumn                 *column.StringBase[string]
	AdFloorThresholdColumn              *column.StringBase[string]
	AdFloorGptColumn                    *column.StringBase[string]
	AdFloorPrebidColumn                 *column.StringBase[string]
	AdFloorAmazonColumn                 *column.StringBase[string]
	AdFloorPboColumn                    *column.StringBase[string]
	AdBuyerIDColumn                     *column.Base[uint16]
	AdBrandIDColumn                     *column.Base[uint32]
	AdAdvertiserDomainColumn            *column.StringBase[string]
	AdDealIDColumn                      *column.StringBase[string]
	AdMediaTypeColumn                   *column.StringBase[string]
	AdUnfilledColumn                    *column.Base[uint8]
	AdSeatIDColumn                      *column.StringBase[string]
	AdSiteIDColumn                      *column.StringBase[string]
	AdQualityBlockingTypeColumn         *column.Base[uint8]
	AdQualityBlockingIDColumn           *column.StringBase[string]
	AdQualityWrapperIDColumn            *column.StringBase[string]
	AdQualityTagIDColumn                *column.StringBase[string]
	AdPlacementIDColumn                 *column.StringBase[string]
	ViewedMeasurableColumn              *column.Base[uint8]
	DfpAdUnitPathColumn                 *column.StringBase[string]
	DfpAdvertiserIDColumn               *column.Base[uint64]
	DfpCampaignIDColumn                 *column.Base[uint64]
	DfpCreativeIDColumn                 *column.Base[uint64]
	DfpLineItemIDColumn                 *column.Base[uint64]
	DfpIsBackfillColumn                 *column.Base[int8]
	DfpConfirmedClickColumn             *column.Base[int8]
	DfpHashColumn                       *column.Base[int16]
	DfpHashRawColumn                    *column.StringBase[string]
	DfpAmazonBidColumn                  *column.StringBase[string]
	DfpAmazonBidderIDColumn             *column.StringBase[string]
	MetaHashColumn                      *column.Base[uint64]
	MetaHashRawColumn                   *column.StringBase[string]
	DaPredictedColumn                   *column.Nullable[uint64]
	DaPredictedServerColumn             *column.Nullable[uint64]
	PrebidHighestBidColumn              *column.Nullable[int64]
	PrebidSecondHighestBidColumn        *column.Nullable[int64]
	PrebidHighestBidPartnerColumn       *column.StringBase[string]
	PrebidOriginalBidderCodeColumn      *column.StringBase[string]
	PrebidCachedBidColumn               *column.Base[uint8]
	PrebidAuctionIDColumn               *column.Base[uint64]
	PrebidWonColumn                     *column.Base[uint8]
	PrebidTimeToRespondColumn           *column.Base[uint16]
	RevenueBiasColumn                   *column.Base[int64]
	PrebidTimeoutColumn                 *column.Base[uint16]
	PrebidUserIdsColumn                 *column.Array[string]
	PrebidConfigUserIdsColumn           *column.Array[string]
	PrebidVersionColumn                 *column.StringBase[string]
	PrebidSlotPreviousHighestBidsColumn *column.Array[int64]
	ApsWonColumn                        *column.Base[uint8]
	ApsPmpWonColumn                     *column.Base[uint8]
	CustomUserStateColumn               *column.StringBase[string]
	CustomLayoutColumn                  *column.StringBase[string]
	Custom1Column                       *column.StringBase[string]
	Custom2Column                       *column.StringBase[string]
	Custom3Column                       *column.StringBase[string]
	Custom4Column                       *column.StringBase[string]
	Custom5Column                       *column.StringBase[string]
	Custom6Column                       *column.StringBase[string]
	Custom7Column                       *column.StringBase[string]
	Custom8Column                       *column.StringBase[string]
	Custom9Column                       *column.StringBase[string]
	Custom10Column                      *column.StringBase[string]
	Custom11Column                      *column.StringBase[string]
	Custom12Column                      *column.StringBase[string]
	Custom13Column                      *column.StringBase[string]
	Custom14Column                      *column.StringBase[string]
	Custom15Column                      *column.StringBase[string]
	ProtocolColumn                      *column.StringBase[string]
	HostColumn                          *column.StringBase[string]
	PathnameColumn                      *column.StringBase[string]
	Pathname1Column                     *column.StringBase[string]
	Pathname2Column                     *column.StringBase[string]
	Pathname3Column                     *column.StringBase[string]
	Pathname4Column                     *column.StringBase[string]
	ReferrerColumn                      *column.StringBase[string]
	UserAgentColumn                     *column.StringBase[string]
	BrowserNameColumn                   *column.StringBase[string]
	BrowserVersionColumn                *column.StringBase[string]
	TimeZoneColumn                      *column.StringBase[string]
	OsNameColumn                        *column.StringBase[string]
	DeviceColumn                        *column.Base[int8]
	DeviceNameColumn                    *column.StringBase[string]
	WindowInnerWidthColumn              *column.Base[int32]
	WindowInnerHeightColumn             *column.Base[int32]
	WindowScrollYColumn                 *column.Base[float32]
	NetworkTypeColumn                   *column.StringBase[string]
	NetworkEffectiveTypeColumn          *column.StringBase[string]
	NetworkDownlinkColumn               *column.Base[int32]
	NetworkRttColumn                    *column.Base[int32]
	NetworkSaveDataColumn               *column.Base[int8]
	IvtCategoryColumn                   *column.Base[int16]
	VitalsLcpColumn                     *column.Base[float32]
	VitalsFidColumn                     *column.Base[float32]
	VitalsClsColumn                     *column.Base[float32]
	AcquisitionClickIDColumn            *column.StringBase[string]
	AcquisitionClickIDParamColumn       *column.StringBase[string]
	AcquisitionCostColumn               *column.Base[uint64]
	AcquisitionCurrencyColumn           *column.StringBase[string]
	ManagerVersionIDColumn              *column.Base[int32]
	ManagerDeployIDColumn               *column.StringBase[string]
	ManagerPercentageColumn             *column.Base[float32]
	ManagerIsPreviewColumn              *column.Base[uint8]
	ContentHeadlineColumn               *column.StringBase[string]
	ContentAuthorColumn                 *column.StringBase[string]
	ContentDatePublishedColumn          *column.Base[DateTime]
	ContentDateModifiedColumn           *column.Base[DateTime]
	CountryCodeColumn                   *column.Base[[2]byte]
	CountryNameColumn                   *column.StringBase[string]
	ContinentCodeColumn                 *column.Base[[2]byte]
	RegionNameColumn                    *column.StringBase[string]
	LanguageColumn                      *column.Base[[2]byte]
	TopicsColumn                        *column.Array[uint16]
	PageViewImpressionCountColumn       *column.Base[uint16]
	PageViewRefreshCountColumn          *column.Base[uint16]
	PageViewIsInitialColumn             *column.Base[int8]
	SessionEntryPageColumn              *column.StringBase[string]
	SessionImpressionCountColumn        *column.Base[uint16]
	SessionPageViewCountColumn          *column.Base[uint16]
	SessionUtmColumn                    *column.Base[uint8]
	SessionUtmSourceColumn              *column.StringBase[string]
	SessionUtmMediumColumn              *column.StringBase[string]
	SessionUtmCampaignColumn            *column.StringBase[string]
	SessionUtmTermColumn                *column.StringBase[string]
	SessionUtmContentColumn             *column.StringBase[string]
	SessionStartTimeColumn              *column.Base[DateTime]
	ClientImpressionCountColumn         *column.Base[uint16]
	ClientPageViewCountColumn           *column.Base[uint16]
	ClientSessionCountColumn            *column.Base[uint16]
	ExternalIDColumn                    *column.StringBase[string]
	ExperimentsMapColumn                *column.Map[string, string]
	EventImpressionColumn               *column.Base[bool]
	EventClickedColumn                  *column.Base[DateTime]
	EventClickBouncedColumn             *column.Base[uint32]
	EventViewedColumn                   *column.Base[DateTime]
	TimestampColumn                     *column.Base[DateTime]
	AssertiveVersionColumn              *column.StringBase[string]
	VColumn                             *column.Base[uint8]
}

func (t Model) ChColumns() column.TupleStruct[Model] {
	return newchtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata()
}

func newchtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata() *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata {
	t := &chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata{}
	t.EntityIDColumn = column.NewStringBase[string]()
	t.EntityIDColumn.SetName([]byte("EntityID"))
	t.EventColumn = column.New[ModelEvent]()
	t.EventColumn.SetName([]byte("Event"))
	t.SourceColumn = column.New[ModelSource]()
	t.SourceColumn.SetName([]byte("Source"))
	t.RevenueColumn = column.New[int64]().Nullable()
	t.RevenueColumn.SetName([]byte("Revenue"))
	t.ClientIDColumn = column.New[uint64]()
	t.ClientIDColumn.SetName([]byte("ClientID"))
	t.SessionIDColumn = column.New[uint64]()
	t.SessionIDColumn.SetName([]byte("SessionID"))
	t.PageViewIDColumn = column.New[uint64]()
	t.PageViewIDColumn.SetName([]byte("PageViewID"))
	t.ImpressionIDColumn = column.New[uint64]()
	t.ImpressionIDColumn.SetName([]byte("ImpressionID"))
	t.AdSlotIDColumn = column.NewStringBase[string]()
	t.AdSlotIDColumn.SetName([]byte("AdSlotID"))
	t.AdSizeColumn = column.NewStringBase[string]()
	t.AdSizeColumn.SetName([]byte("AdSize"))
	t.AdFloorColumn = column.New[uint64]().Nullable()
	t.AdFloorColumn.SetName([]byte("AdFloor"))
	t.AdFloorGroupColumn = column.NewStringBase[string]()
	t.AdFloorGroupColumn.SetName([]byte("AdFloorGroup"))
	t.AdFloorStatusColumn = column.NewStringBase[string]()
	t.AdFloorStatusColumn.SetName([]byte("AdFloorStatus"))
	t.AdFloorThresholdColumn = column.NewStringBase[string]()
	t.AdFloorThresholdColumn.SetName([]byte("AdFloorThreshold"))
	t.AdFloorGptColumn = column.NewStringBase[string]()
	t.AdFloorGptColumn.SetName([]byte("AdFloorGpt"))
	t.AdFloorPrebidColumn = column.NewStringBase[string]()
	t.AdFloorPrebidColumn.SetName([]byte("AdFloorPrebid"))
	t.AdFloorAmazonColumn = column.NewStringBase[string]()
	t.AdFloorAmazonColumn.SetName([]byte("AdFloorAmazon"))
	t.AdFloorPboColumn = column.NewStringBase[string]()
	t.AdFloorPboColumn.SetName([]byte("AdFloorPbo"))
	t.AdBuyerIDColumn = column.New[uint16]()
	t.AdBuyerIDColumn.SetName([]byte("AdBuyerID"))
	t.AdBrandIDColumn = column.New[uint32]()
	t.AdBrandIDColumn.SetName([]byte("AdBrandID"))
	t.AdAdvertiserDomainColumn = column.NewStringBase[string]()
	t.AdAdvertiserDomainColumn.SetName([]byte("AdAdvertiserDomain"))
	t.AdDealIDColumn = column.NewStringBase[string]()
	t.AdDealIDColumn.SetName([]byte("AdDealID"))
	t.AdMediaTypeColumn = column.NewStringBase[string]()
	t.AdMediaTypeColumn.SetName([]byte("AdMediaType"))
	t.AdUnfilledColumn = column.New[uint8]()
	t.AdUnfilledColumn.SetName([]byte("AdUnfilled"))
	t.AdSeatIDColumn = column.NewStringBase[string]()
	t.AdSeatIDColumn.SetName([]byte("AdSeatID"))
	t.AdSiteIDColumn = column.NewStringBase[string]()
	t.AdSiteIDColumn.SetName([]byte("AdSiteID"))
	t.AdQualityBlockingTypeColumn = column.New[uint8]()
	t.AdQualityBlockingTypeColumn.SetName([]byte("AdQualityBlockingType"))
	t.AdQualityBlockingIDColumn = column.NewStringBase[string]()
	t.AdQualityBlockingIDColumn.SetName([]byte("AdQualityBlockingID"))
	t.AdQualityWrapperIDColumn = column.NewStringBase[string]()
	t.AdQualityWrapperIDColumn.SetName([]byte("AdQualityWrapperID"))
	t.AdQualityTagIDColumn = column.NewStringBase[string]()
	t.AdQualityTagIDColumn.SetName([]byte("AdQualityTagID"))
	t.AdPlacementIDColumn = column.NewStringBase[string]()
	t.AdPlacementIDColumn.SetName([]byte("AdPlacementID"))
	t.ViewedMeasurableColumn = column.New[uint8]()
	t.ViewedMeasurableColumn.SetName([]byte("ViewedMeasurable"))
	t.DfpAdUnitPathColumn = column.NewStringBase[string]()
	t.DfpAdUnitPathColumn.SetName([]byte("DfpAdUnitPath"))
	t.DfpAdvertiserIDColumn = column.New[uint64]()
	t.DfpAdvertiserIDColumn.SetName([]byte("DfpAdvertiserID"))
	t.DfpCampaignIDColumn = column.New[uint64]()
	t.DfpCampaignIDColumn.SetName([]byte("DfpCampaignID"))
	t.DfpCreativeIDColumn = column.New[uint64]()
	t.DfpCreativeIDColumn.SetName([]byte("DfpCreativeID"))
	t.DfpLineItemIDColumn = column.New[uint64]()
	t.DfpLineItemIDColumn.SetName([]byte("DfpLineItemID"))
	t.DfpIsBackfillColumn = column.New[int8]()
	t.DfpIsBackfillColumn.SetName([]byte("DfpIsBackfill"))
	t.DfpConfirmedClickColumn = column.New[int8]()
	t.DfpConfirmedClickColumn.SetName([]byte("DfpConfirmedClick"))
	t.DfpHashColumn = column.New[int16]()
	t.DfpHashColumn.SetName([]byte("DfpHash"))
	t.DfpHashRawColumn = column.NewStringBase[string]()
	t.DfpHashRawColumn.SetName([]byte("DfpHashRaw"))
	t.DfpAmazonBidColumn = column.NewStringBase[string]()
	t.DfpAmazonBidColumn.SetName([]byte("DfpAmazonBid"))
	t.DfpAmazonBidderIDColumn = column.NewStringBase[string]()
	t.DfpAmazonBidderIDColumn.SetName([]byte("DfpAmazonBidderID"))
	t.MetaHashColumn = column.New[uint64]()
	t.MetaHashColumn.SetName([]byte("MetaHash"))
	t.MetaHashRawColumn = column.NewStringBase[string]()
	t.MetaHashRawColumn.SetName([]byte("MetaHashRaw"))
	t.DaPredictedColumn = column.New[uint64]().Nullable()
	t.DaPredictedColumn.SetName([]byte("DaPredicted"))
	t.DaPredictedServerColumn = column.New[uint64]().Nullable()
	t.DaPredictedServerColumn.SetName([]byte("DaPredictedServer"))
	t.PrebidHighestBidColumn = column.New[int64]().Nullable()
	t.PrebidHighestBidColumn.SetName([]byte("PrebidHighestBid"))
	t.PrebidSecondHighestBidColumn = column.New[int64]().Nullable()
	t.PrebidSecondHighestBidColumn.SetName([]byte("PrebidSecondHighestBid"))
	t.PrebidHighestBidPartnerColumn = column.NewStringBase[string]()
	t.PrebidHighestBidPartnerColumn.SetName([]byte("PrebidHighestBidPartner"))
	t.PrebidOriginalBidderCodeColumn = column.NewStringBase[string]()
	t.PrebidOriginalBidderCodeColumn.SetName([]byte("PrebidOriginalBidderCode"))
	t.PrebidCachedBidColumn = column.New[uint8]()
	t.PrebidCachedBidColumn.SetName([]byte("PrebidCachedBid"))
	t.PrebidAuctionIDColumn = column.New[uint64]()
	t.PrebidAuctionIDColumn.SetName([]byte("PrebidAuctionID"))
	t.PrebidWonColumn = column.New[uint8]()
	t.PrebidWonColumn.SetName([]byte("PrebidWon"))
	t.PrebidTimeToRespondColumn = column.New[uint16]()
	t.PrebidTimeToRespondColumn.SetName([]byte("PrebidTimeToRespond"))
	t.RevenueBiasColumn = column.New[int64]()
	t.RevenueBiasColumn.SetName([]byte("RevenueBias"))
	t.PrebidTimeoutColumn = column.New[uint16]()
	t.PrebidTimeoutColumn.SetName([]byte("PrebidTimeout"))
	t.PrebidUserIdsColumn = column.NewStringBase[string]().Array()
	t.PrebidUserIdsColumn.SetName([]byte("PrebidUserIds"))
	t.PrebidConfigUserIdsColumn = column.NewStringBase[string]().Array()
	t.PrebidConfigUserIdsColumn.SetName([]byte("PrebidConfigUserIds"))
	t.PrebidVersionColumn = column.NewStringBase[string]()
	t.PrebidVersionColumn.SetName([]byte("PrebidVersion"))
	t.PrebidSlotPreviousHighestBidsColumn = column.New[int64]().Array()
	t.PrebidSlotPreviousHighestBidsColumn.SetName([]byte("PrebidSlotPreviousHighestBids"))
	t.ApsWonColumn = column.New[uint8]()
	t.ApsWonColumn.SetName([]byte("ApsWon"))
	t.ApsPmpWonColumn = column.New[uint8]()
	t.ApsPmpWonColumn.SetName([]byte("ApsPmpWon"))
	t.CustomUserStateColumn = column.NewStringBase[string]()
	t.CustomUserStateColumn.SetName([]byte("CustomUserState"))
	t.CustomLayoutColumn = column.NewStringBase[string]()
	t.CustomLayoutColumn.SetName([]byte("CustomLayout"))
	t.Custom1Column = column.NewStringBase[string]()
	t.Custom1Column.SetName([]byte("Custom1"))
	t.Custom2Column = column.NewStringBase[string]()
	t.Custom2Column.SetName([]byte("Custom2"))
	t.Custom3Column = column.NewStringBase[string]()
	t.Custom3Column.SetName([]byte("Custom3"))
	t.Custom4Column = column.NewStringBase[string]()
	t.Custom4Column.SetName([]byte("Custom4"))
	t.Custom5Column = column.NewStringBase[string]()
	t.Custom5Column.SetName([]byte("Custom5"))
	t.Custom6Column = column.NewStringBase[string]()
	t.Custom6Column.SetName([]byte("Custom6"))
	t.Custom7Column = column.NewStringBase[string]()
	t.Custom7Column.SetName([]byte("Custom7"))
	t.Custom8Column = column.NewStringBase[string]()
	t.Custom8Column.SetName([]byte("Custom8"))
	t.Custom9Column = column.NewStringBase[string]()
	t.Custom9Column.SetName([]byte("Custom9"))
	t.Custom10Column = column.NewStringBase[string]()
	t.Custom10Column.SetName([]byte("Custom10"))
	t.Custom11Column = column.NewStringBase[string]()
	t.Custom11Column.SetName([]byte("Custom11"))
	t.Custom12Column = column.NewStringBase[string]()
	t.Custom12Column.SetName([]byte("Custom12"))
	t.Custom13Column = column.NewStringBase[string]()
	t.Custom13Column.SetName([]byte("Custom13"))
	t.Custom14Column = column.NewStringBase[string]()
	t.Custom14Column.SetName([]byte("Custom14"))
	t.Custom15Column = column.NewStringBase[string]()
	t.Custom15Column.SetName([]byte("Custom15"))
	t.ProtocolColumn = column.NewStringBase[string]()
	t.ProtocolColumn.SetName([]byte("Protocol"))
	t.HostColumn = column.NewStringBase[string]()
	t.HostColumn.SetName([]byte("Host"))
	t.PathnameColumn = column.NewStringBase[string]()
	t.PathnameColumn.SetName([]byte("Pathname"))
	t.Pathname1Column = column.NewStringBase[string]()
	t.Pathname1Column.SetName([]byte("Pathname1"))
	t.Pathname2Column = column.NewStringBase[string]()
	t.Pathname2Column.SetName([]byte("Pathname2"))
	t.Pathname3Column = column.NewStringBase[string]()
	t.Pathname3Column.SetName([]byte("Pathname3"))
	t.Pathname4Column = column.NewStringBase[string]()
	t.Pathname4Column.SetName([]byte("Pathname4"))
	t.ReferrerColumn = column.NewStringBase[string]()
	t.ReferrerColumn.SetName([]byte("Referrer"))
	t.UserAgentColumn = column.NewStringBase[string]()
	t.UserAgentColumn.SetName([]byte("UserAgent"))
	t.BrowserNameColumn = column.NewStringBase[string]()
	t.BrowserNameColumn.SetName([]byte("BrowserName"))
	t.BrowserVersionColumn = column.NewStringBase[string]()
	t.BrowserVersionColumn.SetName([]byte("BrowserVersion"))
	t.TimeZoneColumn = column.NewStringBase[string]()
	t.TimeZoneColumn.SetName([]byte("TimeZone"))
	t.OsNameColumn = column.NewStringBase[string]()
	t.OsNameColumn.SetName([]byte("OsName"))
	t.DeviceColumn = column.New[int8]()
	t.DeviceColumn.SetName([]byte("Device"))
	t.DeviceNameColumn = column.NewStringBase[string]()
	t.DeviceNameColumn.SetName([]byte("DeviceName"))
	t.WindowInnerWidthColumn = column.New[int32]()
	t.WindowInnerWidthColumn.SetName([]byte("WindowInnerWidth"))
	t.WindowInnerHeightColumn = column.New[int32]()
	t.WindowInnerHeightColumn.SetName([]byte("WindowInnerHeight"))
	t.WindowScrollYColumn = column.New[float32]()
	t.WindowScrollYColumn.SetName([]byte("WindowScrollY"))
	t.NetworkTypeColumn = column.NewStringBase[string]()
	t.NetworkTypeColumn.SetName([]byte("NetworkType"))
	t.NetworkEffectiveTypeColumn = column.NewStringBase[string]()
	t.NetworkEffectiveTypeColumn.SetName([]byte("NetworkEffectiveType"))
	t.NetworkDownlinkColumn = column.New[int32]()
	t.NetworkDownlinkColumn.SetName([]byte("NetworkDownlink"))
	t.NetworkRttColumn = column.New[int32]()
	t.NetworkRttColumn.SetName([]byte("NetworkRtt"))
	t.NetworkSaveDataColumn = column.New[int8]()
	t.NetworkSaveDataColumn.SetName([]byte("NetworkSaveData"))
	t.IvtCategoryColumn = column.New[int16]()
	t.IvtCategoryColumn.SetName([]byte("IvtCategory"))
	t.VitalsLcpColumn = column.New[float32]()
	t.VitalsLcpColumn.SetName([]byte("VitalsLcp"))
	t.VitalsFidColumn = column.New[float32]()
	t.VitalsFidColumn.SetName([]byte("VitalsFid"))
	t.VitalsClsColumn = column.New[float32]()
	t.VitalsClsColumn.SetName([]byte("VitalsCls"))
	t.AcquisitionClickIDColumn = column.NewStringBase[string]()
	t.AcquisitionClickIDColumn.SetName([]byte("AcquisitionClickID"))
	t.AcquisitionClickIDParamColumn = column.NewStringBase[string]()
	t.AcquisitionClickIDParamColumn.SetName([]byte("AcquisitionClickIDParam"))
	t.AcquisitionCostColumn = column.New[uint64]()
	t.AcquisitionCostColumn.SetName([]byte("AcquisitionCost"))
	t.AcquisitionCurrencyColumn = column.NewStringBase[string]()
	t.AcquisitionCurrencyColumn.SetName([]byte("AcquisitionCurrency"))
	t.ManagerVersionIDColumn = column.New[int32]()
	t.ManagerVersionIDColumn.SetName([]byte("ManagerVersionID"))
	t.ManagerDeployIDColumn = column.NewStringBase[string]()
	t.ManagerDeployIDColumn.SetName([]byte("ManagerDeployID"))
	t.ManagerPercentageColumn = column.New[float32]()
	t.ManagerPercentageColumn.SetName([]byte("ManagerPercentage"))
	t.ManagerIsPreviewColumn = column.New[uint8]()
	t.ManagerIsPreviewColumn.SetName([]byte("ManagerIsPreview"))
	t.ContentHeadlineColumn = column.NewStringBase[string]()
	t.ContentHeadlineColumn.SetName([]byte("ContentHeadline"))
	t.ContentAuthorColumn = column.NewStringBase[string]()
	t.ContentAuthorColumn.SetName([]byte("ContentAuthor"))
	t.ContentDatePublishedColumn = column.New[DateTime]()
	t.ContentDatePublishedColumn.SetName([]byte("ContentDatePublished"))
	t.ContentDateModifiedColumn = column.New[DateTime]()
	t.ContentDateModifiedColumn.SetName([]byte("ContentDateModified"))
	t.CountryCodeColumn = column.New[[2]byte]()
	t.CountryCodeColumn.SetName([]byte("CountryCode"))
	t.CountryNameColumn = column.NewStringBase[string]()
	t.CountryNameColumn.SetName([]byte("CountryName"))
	t.ContinentCodeColumn = column.New[[2]byte]()
	t.ContinentCodeColumn.SetName([]byte("ContinentCode"))
	t.RegionNameColumn = column.NewStringBase[string]()
	t.RegionNameColumn.SetName([]byte("RegionName"))
	t.LanguageColumn = column.New[[2]byte]()
	t.LanguageColumn.SetName([]byte("Language"))
	t.TopicsColumn = column.New[uint16]().Array()
	t.TopicsColumn.SetName([]byte("Topics"))
	t.PageViewImpressionCountColumn = column.New[uint16]()
	t.PageViewImpressionCountColumn.SetName([]byte("PageViewImpressionCount"))
	t.PageViewRefreshCountColumn = column.New[uint16]()
	t.PageViewRefreshCountColumn.SetName([]byte("PageViewRefreshCount"))
	t.PageViewIsInitialColumn = column.New[int8]()
	t.PageViewIsInitialColumn.SetName([]byte("PageViewIsInitial"))
	t.SessionEntryPageColumn = column.NewStringBase[string]()
	t.SessionEntryPageColumn.SetName([]byte("SessionEntryPage"))
	t.SessionImpressionCountColumn = column.New[uint16]()
	t.SessionImpressionCountColumn.SetName([]byte("SessionImpressionCount"))
	t.SessionPageViewCountColumn = column.New[uint16]()
	t.SessionPageViewCountColumn.SetName([]byte("SessionPageViewCount"))
	t.SessionUtmColumn = column.New[uint8]()
	t.SessionUtmColumn.SetName([]byte("SessionUtm"))
	t.SessionUtmSourceColumn = column.NewStringBase[string]()
	t.SessionUtmSourceColumn.SetName([]byte("SessionUtmSource"))
	t.SessionUtmMediumColumn = column.NewStringBase[string]()
	t.SessionUtmMediumColumn.SetName([]byte("SessionUtmMedium"))
	t.SessionUtmCampaignColumn = column.NewStringBase[string]()
	t.SessionUtmCampaignColumn.SetName([]byte("SessionUtmCampaign"))
	t.SessionUtmTermColumn = column.NewStringBase[string]()
	t.SessionUtmTermColumn.SetName([]byte("SessionUtmTerm"))
	t.SessionUtmContentColumn = column.NewStringBase[string]()
	t.SessionUtmContentColumn.SetName([]byte("SessionUtmContent"))
	t.SessionStartTimeColumn = column.New[DateTime]()
	t.SessionStartTimeColumn.SetName([]byte("SessionStartTime"))
	t.ClientImpressionCountColumn = column.New[uint16]()
	t.ClientImpressionCountColumn.SetName([]byte("ClientImpressionCount"))
	t.ClientPageViewCountColumn = column.New[uint16]()
	t.ClientPageViewCountColumn.SetName([]byte("ClientPageViewCount"))
	t.ClientSessionCountColumn = column.New[uint16]()
	t.ClientSessionCountColumn.SetName([]byte("ClientSessionCount"))
	t.ExternalIDColumn = column.NewStringBase[string]()
	t.ExternalIDColumn.SetName([]byte("ExternalID"))
	var v1 = column.NewStringBase[string]()
	var v2 = column.NewStringBase[string]()
	t.ExperimentsMapColumn = column.NewMap[string, string](v1, v2)
	t.ExperimentsMapColumn.SetName([]byte("ExperimentsMap"))
	t.EventImpressionColumn = column.New[bool]()
	t.EventImpressionColumn.SetName([]byte("EventImpression"))
	t.EventClickedColumn = column.New[DateTime]()
	t.EventClickedColumn.SetName([]byte("EventClicked"))
	t.EventClickBouncedColumn = column.New[uint32]()
	t.EventClickBouncedColumn.SetName([]byte("EventClickBounced"))
	t.EventViewedColumn = column.New[DateTime]()
	t.EventViewedColumn.SetName([]byte("EventViewed"))
	t.TimestampColumn = column.New[DateTime]()
	t.TimestampColumn.SetName([]byte("Timestamp"))
	t.AssertiveVersionColumn = column.NewStringBase[string]()
	t.AssertiveVersionColumn.SetName([]byte("AssertiveVersion"))
	t.VColumn = column.New[uint8]()
	t.VColumn.SetName([]byte("V"))
	t.Tuple = column.NewTuple(
		t.EntityIDColumn,
		t.EventColumn,
		t.SourceColumn,
		t.RevenueColumn,
		t.ClientIDColumn,
		t.SessionIDColumn,
		t.PageViewIDColumn,
		t.ImpressionIDColumn,
		t.AdSlotIDColumn,
		t.AdSizeColumn,
		t.AdFloorColumn,
		t.AdFloorGroupColumn,
		t.AdFloorStatusColumn,
		t.AdFloorThresholdColumn,
		t.AdFloorGptColumn,
		t.AdFloorPrebidColumn,
		t.AdFloorAmazonColumn,
		t.AdFloorPboColumn,
		t.AdBuyerIDColumn,
		t.AdBrandIDColumn,
		t.AdAdvertiserDomainColumn,
		t.AdDealIDColumn,
		t.AdMediaTypeColumn,
		t.AdUnfilledColumn,
		t.AdSeatIDColumn,
		t.AdSiteIDColumn,
		t.AdQualityBlockingTypeColumn,
		t.AdQualityBlockingIDColumn,
		t.AdQualityWrapperIDColumn,
		t.AdQualityTagIDColumn,
		t.AdPlacementIDColumn,
		t.ViewedMeasurableColumn,
		t.DfpAdUnitPathColumn,
		t.DfpAdvertiserIDColumn,
		t.DfpCampaignIDColumn,
		t.DfpCreativeIDColumn,
		t.DfpLineItemIDColumn,
		t.DfpIsBackfillColumn,
		t.DfpConfirmedClickColumn,
		t.DfpHashColumn,
		t.DfpHashRawColumn,
		t.DfpAmazonBidColumn,
		t.DfpAmazonBidderIDColumn,
		t.MetaHashColumn,
		t.MetaHashRawColumn,
		t.DaPredictedColumn,
		t.DaPredictedServerColumn,
		t.PrebidHighestBidColumn,
		t.PrebidSecondHighestBidColumn,
		t.PrebidHighestBidPartnerColumn,
		t.PrebidOriginalBidderCodeColumn,
		t.PrebidCachedBidColumn,
		t.PrebidAuctionIDColumn,
		t.PrebidWonColumn,
		t.PrebidTimeToRespondColumn,
		t.RevenueBiasColumn,
		t.PrebidTimeoutColumn,
		t.PrebidUserIdsColumn,
		t.PrebidConfigUserIdsColumn,
		t.PrebidVersionColumn,
		t.PrebidSlotPreviousHighestBidsColumn,
		t.ApsWonColumn,
		t.ApsPmpWonColumn,
		t.CustomUserStateColumn,
		t.CustomLayoutColumn,
		t.Custom1Column,
		t.Custom2Column,
		t.Custom3Column,
		t.Custom4Column,
		t.Custom5Column,
		t.Custom6Column,
		t.Custom7Column,
		t.Custom8Column,
		t.Custom9Column,
		t.Custom10Column,
		t.Custom11Column,
		t.Custom12Column,
		t.Custom13Column,
		t.Custom14Column,
		t.Custom15Column,
		t.ProtocolColumn,
		t.HostColumn,
		t.PathnameColumn,
		t.Pathname1Column,
		t.Pathname2Column,
		t.Pathname3Column,
		t.Pathname4Column,
		t.ReferrerColumn,
		t.UserAgentColumn,
		t.BrowserNameColumn,
		t.BrowserVersionColumn,
		t.TimeZoneColumn,
		t.OsNameColumn,
		t.DeviceColumn,
		t.DeviceNameColumn,
		t.WindowInnerWidthColumn,
		t.WindowInnerHeightColumn,
		t.WindowScrollYColumn,
		t.NetworkTypeColumn,
		t.NetworkEffectiveTypeColumn,
		t.NetworkDownlinkColumn,
		t.NetworkRttColumn,
		t.NetworkSaveDataColumn,
		t.IvtCategoryColumn,
		t.VitalsLcpColumn,
		t.VitalsFidColumn,
		t.VitalsClsColumn,
		t.AcquisitionClickIDColumn,
		t.AcquisitionClickIDParamColumn,
		t.AcquisitionCostColumn,
		t.AcquisitionCurrencyColumn,
		t.ManagerVersionIDColumn,
		t.ManagerDeployIDColumn,
		t.ManagerPercentageColumn,
		t.ManagerIsPreviewColumn,
		t.ContentHeadlineColumn,
		t.ContentAuthorColumn,
		t.ContentDatePublishedColumn,
		t.ContentDateModifiedColumn,
		t.CountryCodeColumn,
		t.CountryNameColumn,
		t.ContinentCodeColumn,
		t.RegionNameColumn,
		t.LanguageColumn,
		t.TopicsColumn,
		t.PageViewImpressionCountColumn,
		t.PageViewRefreshCountColumn,
		t.PageViewIsInitialColumn,
		t.SessionEntryPageColumn,
		t.SessionImpressionCountColumn,
		t.SessionPageViewCountColumn,
		t.SessionUtmColumn,
		t.SessionUtmSourceColumn,
		t.SessionUtmMediumColumn,
		t.SessionUtmCampaignColumn,
		t.SessionUtmTermColumn,
		t.SessionUtmContentColumn,
		t.SessionStartTimeColumn,
		t.ClientImpressionCountColumn,
		t.ClientPageViewCountColumn,
		t.ClientSessionCountColumn,
		t.ExternalIDColumn,
		t.ExperimentsMapColumn,
		t.EventImpressionColumn,
		t.EventClickedColumn,
		t.EventClickBouncedColumn,
		t.EventViewedColumn,
		t.TimestampColumn,
		t.AssertiveVersionColumn,
		t.VColumn,
	)
	return t
}
func (t *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata) Append(data ...Model) {
	for _, m := range data {
		t.EntityIDColumn.Append(m.EntityID)
		t.EventColumn.Append(m.Event)
		t.SourceColumn.Append(m.Source)
		t.RevenueColumn.AppendP(m.Revenue)
		t.ClientIDColumn.Append(m.ClientID)
		t.SessionIDColumn.Append(m.SessionID)
		t.PageViewIDColumn.Append(m.PageViewID)
		t.ImpressionIDColumn.Append(m.ImpressionID)
		t.AdSlotIDColumn.Append(m.AdSlotID)
		t.AdSizeColumn.Append(m.AdSize)
		t.AdFloorColumn.AppendP(m.AdFloor)
		t.AdFloorGroupColumn.Append(m.AdFloorGroup)
		t.AdFloorStatusColumn.Append(m.AdFloorStatus)
		t.AdFloorThresholdColumn.Append(m.AdFloorThreshold)
		t.AdFloorGptColumn.Append(m.AdFloorGpt)
		t.AdFloorPrebidColumn.Append(m.AdFloorPrebid)
		t.AdFloorAmazonColumn.Append(m.AdFloorAmazon)
		t.AdFloorPboColumn.Append(m.AdFloorPbo)
		t.AdBuyerIDColumn.Append(m.AdBuyerID)
		t.AdBrandIDColumn.Append(m.AdBrandID)
		t.AdAdvertiserDomainColumn.Append(m.AdAdvertiserDomain)
		t.AdDealIDColumn.Append(m.AdDealID)
		t.AdMediaTypeColumn.Append(m.AdMediaType)
		t.AdUnfilledColumn.Append(m.AdUnfilled)
		t.AdSeatIDColumn.Append(m.AdSeatID)
		t.AdSiteIDColumn.Append(m.AdSiteID)
		t.AdQualityBlockingTypeColumn.Append(m.AdQualityBlockingType)
		t.AdQualityBlockingIDColumn.Append(m.AdQualityBlockingID)
		t.AdQualityWrapperIDColumn.Append(m.AdQualityWrapperID)
		t.AdQualityTagIDColumn.Append(m.AdQualityTagID)
		t.AdPlacementIDColumn.Append(m.AdPlacementID)
		t.ViewedMeasurableColumn.Append(m.ViewedMeasurable)
		t.DfpAdUnitPathColumn.Append(m.DfpAdUnitPath)
		t.DfpAdvertiserIDColumn.Append(m.DfpAdvertiserID)
		t.DfpCampaignIDColumn.Append(m.DfpCampaignID)
		t.DfpCreativeIDColumn.Append(m.DfpCreativeID)
		t.DfpLineItemIDColumn.Append(m.DfpLineItemID)
		t.DfpIsBackfillColumn.Append(m.DfpIsBackfill)
		t.DfpConfirmedClickColumn.Append(m.DfpConfirmedClick)
		t.DfpHashColumn.Append(m.DfpHash)
		t.DfpHashRawColumn.Append(m.DfpHashRaw)
		t.DfpAmazonBidColumn.Append(m.DfpAmazonBid)
		t.DfpAmazonBidderIDColumn.Append(m.DfpAmazonBidderID)
		t.MetaHashColumn.Append(m.MetaHash)
		t.MetaHashRawColumn.Append(m.MetaHashRaw)
		t.DaPredictedColumn.AppendP(m.DaPredicted)
		t.DaPredictedServerColumn.AppendP(m.DaPredictedServer)
		t.PrebidHighestBidColumn.AppendP(m.PrebidHighestBid)
		t.PrebidSecondHighestBidColumn.AppendP(m.PrebidSecondHighestBid)
		t.PrebidHighestBidPartnerColumn.Append(m.PrebidHighestBidPartner)
		t.PrebidOriginalBidderCodeColumn.Append(m.PrebidOriginalBidderCode)
		t.PrebidCachedBidColumn.Append(m.PrebidCachedBid)
		t.PrebidAuctionIDColumn.Append(m.PrebidAuctionID)
		t.PrebidWonColumn.Append(m.PrebidWon)
		t.PrebidTimeToRespondColumn.Append(m.PrebidTimeToRespond)
		t.RevenueBiasColumn.Append(m.RevenueBias)
		t.PrebidTimeoutColumn.Append(m.PrebidTimeout)
		t.PrebidUserIdsColumn.Append(m.PrebidUserIds)
		t.PrebidConfigUserIdsColumn.Append(m.PrebidConfigUserIds)
		t.PrebidVersionColumn.Append(m.PrebidVersion)
		t.PrebidSlotPreviousHighestBidsColumn.Append(m.PrebidSlotPreviousHighestBids)
		t.ApsWonColumn.Append(m.ApsWon)
		t.ApsPmpWonColumn.Append(m.ApsPmpWon)
		t.CustomUserStateColumn.Append(m.CustomUserState)
		t.CustomLayoutColumn.Append(m.CustomLayout)
		t.Custom1Column.Append(m.Custom1)
		t.Custom2Column.Append(m.Custom2)
		t.Custom3Column.Append(m.Custom3)
		t.Custom4Column.Append(m.Custom4)
		t.Custom5Column.Append(m.Custom5)
		t.Custom6Column.Append(m.Custom6)
		t.Custom7Column.Append(m.Custom7)
		t.Custom8Column.Append(m.Custom8)
		t.Custom9Column.Append(m.Custom9)
		t.Custom10Column.Append(m.Custom10)
		t.Custom11Column.Append(m.Custom11)
		t.Custom12Column.Append(m.Custom12)
		t.Custom13Column.Append(m.Custom13)
		t.Custom14Column.Append(m.Custom14)
		t.Custom15Column.Append(m.Custom15)
		t.ProtocolColumn.Append(m.Protocol)
		t.HostColumn.Append(m.Host)
		t.PathnameColumn.Append(m.Pathname)
		t.Pathname1Column.Append(m.Pathname1)
		t.Pathname2Column.Append(m.Pathname2)
		t.Pathname3Column.Append(m.Pathname3)
		t.Pathname4Column.Append(m.Pathname4)
		t.ReferrerColumn.Append(m.Referrer)
		t.UserAgentColumn.Append(m.UserAgent)
		t.BrowserNameColumn.Append(m.BrowserName)
		t.BrowserVersionColumn.Append(m.BrowserVersion)
		t.TimeZoneColumn.Append(m.TimeZone)
		t.OsNameColumn.Append(m.OsName)
		t.DeviceColumn.Append(m.Device)
		t.DeviceNameColumn.Append(m.DeviceName)
		t.WindowInnerWidthColumn.Append(m.WindowInnerWidth)
		t.WindowInnerHeightColumn.Append(m.WindowInnerHeight)
		t.WindowScrollYColumn.Append(m.WindowScrollY)
		t.NetworkTypeColumn.Append(m.NetworkType)
		t.NetworkEffectiveTypeColumn.Append(m.NetworkEffectiveType)
		t.NetworkDownlinkColumn.Append(m.NetworkDownlink)
		t.NetworkRttColumn.Append(m.NetworkRtt)
		t.NetworkSaveDataColumn.Append(m.NetworkSaveData)
		t.IvtCategoryColumn.Append(m.IvtCategory)
		t.VitalsLcpColumn.Append(m.VitalsLcp)
		t.VitalsFidColumn.Append(m.VitalsFid)
		t.VitalsClsColumn.Append(m.VitalsCls)
		t.AcquisitionClickIDColumn.Append(m.AcquisitionClickID)
		t.AcquisitionClickIDParamColumn.Append(m.AcquisitionClickIDParam)
		t.AcquisitionCostColumn.Append(m.AcquisitionCost)
		t.AcquisitionCurrencyColumn.Append(m.AcquisitionCurrency)
		t.ManagerVersionIDColumn.Append(m.ManagerVersionID)
		t.ManagerDeployIDColumn.Append(m.ManagerDeployID)
		t.ManagerPercentageColumn.Append(m.ManagerPercentage)
		t.ManagerIsPreviewColumn.Append(m.ManagerIsPreview)
		t.ContentHeadlineColumn.Append(m.ContentHeadline)
		t.ContentAuthorColumn.Append(m.ContentAuthor)
		t.ContentDatePublishedColumn.Append(m.ContentDatePublished)
		t.ContentDateModifiedColumn.Append(m.ContentDateModified)
		t.CountryCodeColumn.Append(m.CountryCode)
		t.CountryNameColumn.Append(m.CountryName)
		t.ContinentCodeColumn.Append(m.ContinentCode)
		t.RegionNameColumn.Append(m.RegionName)
		t.LanguageColumn.Append(m.Language)
		t.TopicsColumn.Append(m.Topics)
		t.PageViewImpressionCountColumn.Append(m.PageViewImpressionCount)
		t.PageViewRefreshCountColumn.Append(m.PageViewRefreshCount)
		t.PageViewIsInitialColumn.Append(m.PageViewIsInitial)
		t.SessionEntryPageColumn.Append(m.SessionEntryPage)
		t.SessionImpressionCountColumn.Append(m.SessionImpressionCount)
		t.SessionPageViewCountColumn.Append(m.SessionPageViewCount)
		t.SessionUtmColumn.Append(m.SessionUtm)
		t.SessionUtmSourceColumn.Append(m.SessionUtmSource)
		t.SessionUtmMediumColumn.Append(m.SessionUtmMedium)
		t.SessionUtmCampaignColumn.Append(m.SessionUtmCampaign)
		t.SessionUtmTermColumn.Append(m.SessionUtmTerm)
		t.SessionUtmContentColumn.Append(m.SessionUtmContent)
		t.SessionStartTimeColumn.Append(m.SessionStartTime)
		t.ClientImpressionCountColumn.Append(m.ClientImpressionCount)
		t.ClientPageViewCountColumn.Append(m.ClientPageViewCount)
		t.ClientSessionCountColumn.Append(m.ClientSessionCount)
		t.ExternalIDColumn.Append(m.ExternalID)
		t.ExperimentsMapColumn.Append(m.ExperimentsMap)
		t.EventImpressionColumn.Append(m.EventImpression)
		t.EventClickedColumn.Append(m.EventClicked)
		t.EventClickBouncedColumn.Append(m.EventClickBounced)
		t.EventViewedColumn.Append(m.EventViewed)
		t.TimestampColumn.Append(m.Timestamp)
		t.AssertiveVersionColumn.Append(m.AssertiveVersion)
		t.VColumn.Append(m.V)
	}
}
func (t *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata) Array() *column.Array[Model] {
	return column.NewArray[Model](t)
}

func (t *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata) Data() []Model {
	val := make([]Model, t.NumRow())
	for i := 0; i < t.NumRow(); i++ {
		val[i] = t.Row(i)
	}
	return val
}

func (t *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata) Read(value []Model) []Model {
	if cap(value)-len(value) >= t.NumRow() {
		value = value[:len(value)+t.NumRow()]
	} else {
		value = append(value, make([]Model, t.NumRow())...)
	}

	val := value[len(value)-t.NumRow():]
	for i := 0; i < t.NumRow(); i++ {
		val[i] = t.Row(i)
	}
	return value
}

func (t *chtuplegenBd7fc087ChtuplegenGithubComVahidSohrablooChconnV3Testdata) Row(row int) Model {
	return Model{
		EntityID:                      t.EntityIDColumn.Row(row),
		Event:                         t.EventColumn.Row(row),
		Source:                        t.SourceColumn.Row(row),
		Revenue:                       t.RevenueColumn.RowP(row),
		ClientID:                      t.ClientIDColumn.Row(row),
		SessionID:                     t.SessionIDColumn.Row(row),
		PageViewID:                    t.PageViewIDColumn.Row(row),
		ImpressionID:                  t.ImpressionIDColumn.Row(row),
		AdSlotID:                      t.AdSlotIDColumn.Row(row),
		AdSize:                        t.AdSizeColumn.Row(row),
		AdFloor:                       t.AdFloorColumn.RowP(row),
		AdFloorGroup:                  t.AdFloorGroupColumn.Row(row),
		AdFloorStatus:                 t.AdFloorStatusColumn.Row(row),
		AdFloorThreshold:              t.AdFloorThresholdColumn.Row(row),
		AdFloorGpt:                    t.AdFloorGptColumn.Row(row),
		AdFloorPrebid:                 t.AdFloorPrebidColumn.Row(row),
		AdFloorAmazon:                 t.AdFloorAmazonColumn.Row(row),
		AdFloorPbo:                    t.AdFloorPboColumn.Row(row),
		AdBuyerID:                     t.AdBuyerIDColumn.Row(row),
		AdBrandID:                     t.AdBrandIDColumn.Row(row),
		AdAdvertiserDomain:            t.AdAdvertiserDomainColumn.Row(row),
		AdDealID:                      t.AdDealIDColumn.Row(row),
		AdMediaType:                   t.AdMediaTypeColumn.Row(row),
		AdUnfilled:                    t.AdUnfilledColumn.Row(row),
		AdSeatID:                      t.AdSeatIDColumn.Row(row),
		AdSiteID:                      t.AdSiteIDColumn.Row(row),
		AdQualityBlockingType:         t.AdQualityBlockingTypeColumn.Row(row),
		AdQualityBlockingID:           t.AdQualityBlockingIDColumn.Row(row),
		AdQualityWrapperID:            t.AdQualityWrapperIDColumn.Row(row),
		AdQualityTagID:                t.AdQualityTagIDColumn.Row(row),
		AdPlacementID:                 t.AdPlacementIDColumn.Row(row),
		ViewedMeasurable:              t.ViewedMeasurableColumn.Row(row),
		DfpAdUnitPath:                 t.DfpAdUnitPathColumn.Row(row),
		DfpAdvertiserID:               t.DfpAdvertiserIDColumn.Row(row),
		DfpCampaignID:                 t.DfpCampaignIDColumn.Row(row),
		DfpCreativeID:                 t.DfpCreativeIDColumn.Row(row),
		DfpLineItemID:                 t.DfpLineItemIDColumn.Row(row),
		DfpIsBackfill:                 t.DfpIsBackfillColumn.Row(row),
		DfpConfirmedClick:             t.DfpConfirmedClickColumn.Row(row),
		DfpHash:                       t.DfpHashColumn.Row(row),
		DfpHashRaw:                    t.DfpHashRawColumn.Row(row),
		DfpAmazonBid:                  t.DfpAmazonBidColumn.Row(row),
		DfpAmazonBidderID:             t.DfpAmazonBidderIDColumn.Row(row),
		MetaHash:                      t.MetaHashColumn.Row(row),
		MetaHashRaw:                   t.MetaHashRawColumn.Row(row),
		DaPredicted:                   t.DaPredictedColumn.RowP(row),
		DaPredictedServer:             t.DaPredictedServerColumn.RowP(row),
		PrebidHighestBid:              t.PrebidHighestBidColumn.RowP(row),
		PrebidSecondHighestBid:        t.PrebidSecondHighestBidColumn.RowP(row),
		PrebidHighestBidPartner:       t.PrebidHighestBidPartnerColumn.Row(row),
		PrebidOriginalBidderCode:      t.PrebidOriginalBidderCodeColumn.Row(row),
		PrebidCachedBid:               t.PrebidCachedBidColumn.Row(row),
		PrebidAuctionID:               t.PrebidAuctionIDColumn.Row(row),
		PrebidWon:                     t.PrebidWonColumn.Row(row),
		PrebidTimeToRespond:           t.PrebidTimeToRespondColumn.Row(row),
		RevenueBias:                   t.RevenueBiasColumn.Row(row),
		PrebidTimeout:                 t.PrebidTimeoutColumn.Row(row),
		PrebidUserIds:                 t.PrebidUserIdsColumn.Row(row),
		PrebidConfigUserIds:           t.PrebidConfigUserIdsColumn.Row(row),
		PrebidVersion:                 t.PrebidVersionColumn.Row(row),
		PrebidSlotPreviousHighestBids: t.PrebidSlotPreviousHighestBidsColumn.Row(row),
		ApsWon:                        t.ApsWonColumn.Row(row),
		ApsPmpWon:                     t.ApsPmpWonColumn.Row(row),
		CustomUserState:               t.CustomUserStateColumn.Row(row),
		CustomLayout:                  t.CustomLayoutColumn.Row(row),
		Custom1:                       t.Custom1Column.Row(row),
		Custom2:                       t.Custom2Column.Row(row),
		Custom3:                       t.Custom3Column.Row(row),
		Custom4:                       t.Custom4Column.Row(row),
		Custom5:                       t.Custom5Column.Row(row),
		Custom6:                       t.Custom6Column.Row(row),
		Custom7:                       t.Custom7Column.Row(row),
		Custom8:                       t.Custom8Column.Row(row),
		Custom9:                       t.Custom9Column.Row(row),
		Custom10:                      t.Custom10Column.Row(row),
		Custom11:                      t.Custom11Column.Row(row),
		Custom12:                      t.Custom12Column.Row(row),
		Custom13:                      t.Custom13Column.Row(row),
		Custom14:                      t.Custom14Column.Row(row),
		Custom15:                      t.Custom15Column.Row(row),
		Protocol:                      t.ProtocolColumn.Row(row),
		Host:                          t.HostColumn.Row(row),
		Pathname:                      t.PathnameColumn.Row(row),
		Pathname1:                     t.Pathname1Column.Row(row),
		Pathname2:                     t.Pathname2Column.Row(row),
		Pathname3:                     t.Pathname3Column.Row(row),
		Pathname4:                     t.Pathname4Column.Row(row),
		Referrer:                      t.ReferrerColumn.Row(row),
		UserAgent:                     t.UserAgentColumn.Row(row),
		BrowserName:                   t.BrowserNameColumn.Row(row),
		BrowserVersion:                t.BrowserVersionColumn.Row(row),
		TimeZone:                      t.TimeZoneColumn.Row(row),
		OsName:                        t.OsNameColumn.Row(row),
		Device:                        t.DeviceColumn.Row(row),
		DeviceName:                    t.DeviceNameColumn.Row(row),
		WindowInnerWidth:              t.WindowInnerWidthColumn.Row(row),
		WindowInnerHeight:             t.WindowInnerHeightColumn.Row(row),
		WindowScrollY:                 t.WindowScrollYColumn.Row(row),
		NetworkType:                   t.NetworkTypeColumn.Row(row),
		NetworkEffectiveType:          t.NetworkEffectiveTypeColumn.Row(row),
		NetworkDownlink:               t.NetworkDownlinkColumn.Row(row),
		NetworkRtt:                    t.NetworkRttColumn.Row(row),
		NetworkSaveData:               t.NetworkSaveDataColumn.Row(row),
		IvtCategory:                   t.IvtCategoryColumn.Row(row),
		VitalsLcp:                     t.VitalsLcpColumn.Row(row),
		VitalsFid:                     t.VitalsFidColumn.Row(row),
		VitalsCls:                     t.VitalsClsColumn.Row(row),
		AcquisitionClickID:            t.AcquisitionClickIDColumn.Row(row),
		AcquisitionClickIDParam:       t.AcquisitionClickIDParamColumn.Row(row),
		AcquisitionCost:               t.AcquisitionCostColumn.Row(row),
		AcquisitionCurrency:           t.AcquisitionCurrencyColumn.Row(row),
		ManagerVersionID:              t.ManagerVersionIDColumn.Row(row),
		ManagerDeployID:               t.ManagerDeployIDColumn.Row(row),
		ManagerPercentage:             t.ManagerPercentageColumn.Row(row),
		ManagerIsPreview:              t.ManagerIsPreviewColumn.Row(row),
		ContentHeadline:               t.ContentHeadlineColumn.Row(row),
		ContentAuthor:                 t.ContentAuthorColumn.Row(row),
		ContentDatePublished:          t.ContentDatePublishedColumn.Row(row),
		ContentDateModified:           t.ContentDateModifiedColumn.Row(row),
		CountryCode:                   t.CountryCodeColumn.Row(row),
		CountryName:                   t.CountryNameColumn.Row(row),
		ContinentCode:                 t.ContinentCodeColumn.Row(row),
		RegionName:                    t.RegionNameColumn.Row(row),
		Language:                      t.LanguageColumn.Row(row),
		Topics:                        t.TopicsColumn.Row(row),
		PageViewImpressionCount:       t.PageViewImpressionCountColumn.Row(row),
		PageViewRefreshCount:          t.PageViewRefreshCountColumn.Row(row),
		PageViewIsInitial:             t.PageViewIsInitialColumn.Row(row),
		SessionEntryPage:              t.SessionEntryPageColumn.Row(row),
		SessionImpressionCount:        t.SessionImpressionCountColumn.Row(row),
		SessionPageViewCount:          t.SessionPageViewCountColumn.Row(row),
		SessionUtm:                    t.SessionUtmColumn.Row(row),
		SessionUtmSource:              t.SessionUtmSourceColumn.Row(row),
		SessionUtmMedium:              t.SessionUtmMediumColumn.Row(row),
		SessionUtmCampaign:            t.SessionUtmCampaignColumn.Row(row),
		SessionUtmTerm:                t.SessionUtmTermColumn.Row(row),
		SessionUtmContent:             t.SessionUtmContentColumn.Row(row),
		SessionStartTime:              t.SessionStartTimeColumn.Row(row),
		ClientImpressionCount:         t.ClientImpressionCountColumn.Row(row),
		ClientPageViewCount:           t.ClientPageViewCountColumn.Row(row),
		ClientSessionCount:            t.ClientSessionCountColumn.Row(row),
		ExternalID:                    t.ExternalIDColumn.Row(row),
		ExperimentsMap:                t.ExperimentsMapColumn.Row(row),
		EventImpression:               t.EventImpressionColumn.Row(row),
		EventClicked:                  t.EventClickedColumn.Row(row),
		EventClickBounced:             t.EventClickBouncedColumn.Row(row),
		EventViewed:                   t.EventViewedColumn.Row(row),
		Timestamp:                     t.TimestampColumn.Row(row),
		AssertiveVersion:              t.AssertiveVersionColumn.Row(row),
		V:                             t.VColumn.Row(row),
	}
}
