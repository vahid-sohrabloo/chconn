package helper

const (
	DbmsMinRevisionWithClientInfo                     = 54032
	DbmsMinRevisionWithServerTimezone                 = 54058
	DbmsMinRevisionWithQuotaKeyInClientInfo           = 54060
	DbmsMinRevisionWithServerDisplayName              = 54372
	DbmsMinRevisionWithVersionPatch                   = 54401
	DbmsMinRevisionWithClientWriteInfo                = 54420
	DbmsMinRevisionWithSettingsSerializedAsStrings    = 54429
	DbmsMinRevisionWithInterServerSecret              = 54441
	DbmsMinRevisionWithOpenTelemetry                  = 54442
	DbmsMinProtocolVersionWithDistributedDepth        = 54448
	DbmsMinProtocolVersionWithInitialQueryStartTime   = 54449
	DbmsMinProtocolVersionWithParallelReplicas        = 54453
	DbmsMinProtocolWithCustomSerialization            = 54454
	DbmsMinProtocolWithQuotaKey                       = 54458
	DbmsMinProtocolWithParameters                     = 54459
	DbmsMinProtocolWithServerQueryTimeInProgress      = 54460
	DbmsMinProtocolVersionWithPasswordComplexityRules = 54461
	DbmsMinRevisionWithInterserverSecretV2            = 54462
	DbmsMinProtocolVersionWithTotalBytesInProgress    = 54463
	DbmsMinProtocolVersionWithTimezoneUpdates         = 54464
	DbmsMinRevisionWithSparseSerialization            = 54465

	ClientTCPVersion = DbmsMinRevisionWithSparseSerialization
)
