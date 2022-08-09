from enum import Enum
from sys import getsizeof

PackageStartCookie: int = 0xAFFEBAD0
PackageEndCookie: int = 0xAFFEBADF
EncryptionSize: int = 4
TcpPacketLength: int = getsizeof(int)
PackageStartCookieSize: int = getsizeof(PackageStartCookie)
PackageEndCookieSize: int = getsizeof(PackageEndCookie)
HeaderSize: int = 40
FrameDataMax: int = 1472
DataLengthMax: int = FrameDataMax - PackageStartCookieSize - TcpPacketLength - HeaderSize - PackageEndCookieSize - EncryptionSize
MonitoringValuesMax: int = int((DataLengthMax - 6 * getsizeof(int)) / getsizeof([]))
MonitoringValuesReqMax: int = int((DataLengthMax - 6 * getsizeof(int)) / (2 * getsizeof([]) + getsizeof(int)))
StringSizeMax: int = 80
PathLengthMax: int = 260
EncryptionKey = 0x84CA06031EAA6196
ChannelNameSizeMax: int = 256
DataLoggerChannelsMax: int = 128


class CommunicationPacketHeader:
    version: int
    destination: int
    source: int
    id: int
    command: int
    status: int
    control: int
    extension: int
    size: int
    fragmentCount: int
    fragmentProgress: int


class CommunicationPacket:
    header: CommunicationPacketHeader
    data: [] = [DataLengthMax]


class ExtensionFlags(Enum):
    WITH_ENCRYPTION = 0
    FLAG_1 = 1
    FLAG_2 = 2
    FLAG_3 = 3
    FLAG_4 = 4
    FLAG_5 = 5
    FLAG_6 = 6
    FLAG_7 = 7
    FLAG_8 = 8
    FLAG_9 = 9
    FLAG_10 = 10
    FLAG_11 = 11
    FLAG_12 = 12
    FLAG_13 = 13
    FLAG_14 = 14
    FLAG_15 = 15
    FLAG_16 = 16
    FLAG_17 = 17
    FLAG_18 = 18
    FLAG_19 = 19
    FLAG_20 = 20
    FLAG_21 = 21
    FLAG_22 = 22
    FLAG_23 = 23
    FLAG_24 = 24
    FLAG_25 = 25
    FLAG_26 = 26
    FLAG_27 = 27
    FLAG_28 = 28
    FLAG_29 = 29
    FLAG_30 = 30
    FLAG_31 = 31


class CommunicationCommands(Enum):
    NONE_REQ = 0x00000000
    NONE_CNF = 0x00000001
    LIFE_CHECK_REQ = 0x00000002
    LiFE_CHECK_CNF = 0x00000003
    LIFE_CHECK_CONFIG_REQ = 0x00000004
    LIFE_CHECK_CONFIG_CNF = 0x00000005
    WRITE_FILE_REQ = 0x00000006
    WRITE_FILE_CNF = 0x00000007
    READ_FILE_REQ = 0x00000008
    READ_FILE_CNF = 0x00000009
    READ_DIR_REQ = 0x0000000A
    READ_DIR_CNF = 0x0000000B
    SCOPE_START_REQ = 0x0000000D
    SCOPE_START_CNF = 0x0000000E
    SCOPE_DATA_REQ = 0x0000000F
    SCOPE_DATA_CNF = 0x00000010
    SCOPE_CONFIGURATION_REG = 0x00000011
    SCOPE_CONFIGURATION_CNF = 0x00000012
    SUBSCRIBE_COMMAND_REQ = 0x00000013
    SUBSCRIBE_COMMAND_CNF = 0x00000014
    LAST_COMMAND = 0x00000015


class CommunicationStatus(Enum):
    OK = 0x00000000
    UNKNOWN_COMMAND = 0x00000001
    UNKNOWN_DESTINATION = 0x00000002
    UNKNOWN_SOURCE = 0x00000003
    INVALID_PACKAGE_LENGTH = 0x00000004
    INVALID_EXTENSION = 0x00000005
    INVALID_PARAMETER = 0x00000006
    WATCHDOG_TIMEOUT = 0x000000007
    REQUEST_RUNNING = 0x00000008

    COMMAND_INT = 0x00001000
    COMMAND_STARTED = 0x00001001
    COMMAND_RUNNING = 0x00001002
    COMMAND_ABORT = 0x00001003
    # COMMAND_BUSY = 0x00001100
    COMMAND_FINISH = 0x00001004
    COMMAND_ERROR = 0x00001005
    COMMAND_FINISHED = 0x00001006
    OTHER_COMMAND_IS_RUNNING = 0x00001007
    WRONG_DRIVE_NUMBER = 0x000001008
    DRIVER_NOT_INITIALIZED = 0x00001009
    MONITORING_CONFIG_INVALID = 0x00005000


# # erstmal uninteressant DataLogger Sachen!!!
class ScopeControlReqData:
    startSample: int
    # // Aufzeichnung starten
    dataAvailableAck: int
    # // Signal an Qt, dass neue Daten vorhanden sind
    sampleRate: int
    # // Signal an Qt, dass neue Daten vorhanden sind
    currentConfig: int


class ScopeControlReq:
    header: CommunicationPacketHeader
    data: ScopeControlReqData


class ScopeReadChannelNamesReqData:
    currentConfiguration: int


class ScopeReadChannelNamesCnfData:
    currentConfiguration: int
    namesArray = [DataLoggerChannelsMax]
    # [ChannelNameSizeMax]


class ScopeReadChannelReqNames:
    header: CommunicationPacketHeader
    data: ScopeReadChannelNamesReqData


class ScopeReadChannelCnfNames:
    header: CommunicationPacketHeader
    data: ScopeReadChannelNamesCnfData


class ScopeControlStartCnfData:
    dataAvailable: int
    systemCycleTime: int
    state: int
    currentConfig: int
    sampleRate: int
    status: int
    # // 0 - disabled, 1 - enabled, 2 - started
