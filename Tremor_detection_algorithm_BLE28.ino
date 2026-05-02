/*
 * Tremor_detection_algorithm_BLE28.ino
 *
 * Merged firmware for iGest v3 wearable tremor/anxiety detection system.
 * Combines BLE26 (real-time detection + flash recording) with BLE27
 * (flash-over-BLE sync) into a single non-blocking state machine.
 *
 * Device : LilyGo T-Watch 2020 V3 (ESP32 + BMA423 + PCF8563 + AXP202)
 * BLE Name : iGest v3
 * Passcode  : 6373
 */

// ─────────────────────────────────────────────────────────────────────────────
// Section 1: Includes
// ─────────────────────────────────────────────────────────────────────────────
#include <BLE2902.h>
#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEServer.h>

#include <esp_partition.h>
#include <esp_spi_flash.h>

#include "config.h"

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/semphr.h>

// ─────────────────────────────────────────────────────────────────────────────
// Section 2: Constants & Defines
// ─────────────────────────────────────────────────────────────────────────────

// ── Timing ───────────────────────────────────────────────────────────────────
#define SAMPLE_INTERVAL_MS        10
#define T1_INTERVAL_MS            (15UL * 60UL * 1000UL)
#define T2_SYNC_INTERVAL_MS       50
#define BLE_DURATION              (30UL * 1000UL)

// ── Buffer sizes ──────────────────────────────────────────────────────────────
#define B1_SAMPLES                300
#define B1_RECORD_SIZE            9
#define B1_BUFFER_SIZE            (B1_SAMPLES * B1_RECORD_SIZE)

#define B2_SAMPLES                34
#define B2_RECORD_SIZE            7
#define B2_BUFFER_SIZE            (B2_SAMPLES * B2_RECORD_SIZE)

#define B3_RECORDS_PER_PKT        27
#define B3_PKT_SIZE               (B3_RECORDS_PER_PKT * B1_RECORD_SIZE)

// ── Flash layout ─────────────────────────────────────────────────────────────
#define FLASH_MAGIC               0xACCE1DA7
#define FLASH_CONFIG_MAGIC        0xCF28CF07
#define FLASH_SECTOR_SIZE         4096
#define FLASH_OFFSET_HDR_START    0
#define FLASH_CONFIG_HDR_START    2048
#define FLASH_DATA_START          4096
#define FLASH_MAX_BYTES           3200000UL
#define FLASH_WARN_THRESHOLD      (FLASH_MAX_BYTES * 9 / 10)
#define MAX_HEADER_SLOTS          511   // usable offset slots in Half 1
#define MAX_CONFIG_SLOTS          102   // usable config slots in Half 2

// ── Thresholds ────────────────────────────────────────────────────────────────
#define DEFAULT_TREMOR_THRESHOLD  100.0f
#define DEFAULT_ANXIETY_THRESHOLD 200.0f
#define DEFAULT_THRESHOLD         100.0f
#define CROSSING_THRESHOLD        6
#define MAX_DATA_POINTS           210
#define BASELINE_TIMEFRAME        2.0f
#define CROSSING_TIMEFRAME        1.0f
#define IIR_ORDER                 8
#define IMU_AXIS                  3

// ── Modes ─────────────────────────────────────────────────────────────────────
#define MODE_TREMOR               0
#define MODE_ANXIETY              1

// ── Display ──────────────────────────────────────────────────────────────────
#define DISPLAY_ON_WINDOW_MS      5000
#define DOUBLE_TAP_GAP_MS         350
#define TAP_DEBOUNCE_MS           120

// ── Markers ───────────────────────────────────────────────────────────────────
#define MARKER_TREMOR_START_H1    0x89
#define MARKER_TREMOR_START_H2    0x67
#define MARKER_TREMOR_END_H1      0xCD
#define MARKER_TREMOR_END_H2      0xAB
#define MARKER_ZERO_H1            0x34
#define MARKER_ZERO_H2            0x12
#define MARKER_B2_START_H1        0xB2
#define MARKER_B2_START_H2        0xB2
#define MARKER_B2_END_H1          0xB3
#define MARKER_B2_END_H2          0xB3

static const uint8_t SYNC_BLE_START[4] = {0xAA, 0xBB, 0xCC, 0xDD};
static const uint8_t SYNC_BLE_END[4]   = {0xFF, 0xEE, 0xDD, 0xCC};

// ── UUIDs ─────────────────────────────────────────────────────────────────────
#define IMU_SERVICE_UUID          "19B10000-E8F2-537E-4F6C-D104768A1214"
#define IMU_CHARACTERISTIC_UUID   "19B10001-E8F2-537E-4F6C-D104768A1214"
#define TIME_SERVICE_UUID         "19B20000-E8F2-537E-4F6C-D104768A1214"
#define TIME_CHARACTERISTIC_UUID  "19B20001-E8F2-537E-4F6C-D104768A1214"
#define FLASH_SERVICE_UUID        "19B30000-E8F2-537E-4F6C-D104768A1214"
#define FLASH_DATA_CHAR_UUID      "19B30001-E8F2-537E-4F6C-D104768A1214"
#define FLASH_CMD_CHAR_UUID       "19B30002-E8F2-537E-4F6C-D104768A1214"

// ─────────────────────────────────────────────────────────────────────────────
// Section 3: Hardware Objects
// ─────────────────────────────────────────────────────────────────────────────
TTGOClass *watch;
TFT_eSPI  *tft;
BMA       *sensor;

// ─────────────────────────────────────────────────────────────────────────────
// Section 4: BLE Objects
// ─────────────────────────────────────────────────────────────────────────────
BLEServer*         pServer                = nullptr;
BLEService*        imuService             = nullptr;
BLECharacteristic* imuCharacteristic      = nullptr;
BLEService*        timeService            = nullptr;
BLECharacteristic* timeCharacteristic     = nullptr;
BLEService*        flashService           = nullptr;
BLECharacteristic* flashDataCharacteristic = nullptr;
BLECharacteristic* flashCmdCharacteristic  = nullptr;

// ─────────────────────────────────────────────────────────────────────────────
// Section 5: Sync State Enum & Config Struct
// ─────────────────────────────────────────────────────────────────────────────
typedef enum {
  SYNC_IDLE,
  SYNC_WAITING_BLE,
  SYNC_STARTING,
  SYNC_ACTIVE,
  SYNC_INTERRUPTED,
  SYNC_COMPLETE
} SyncState;

struct ConfigSlot {
  uint8_t  last_mode;          // 0=TREMOR, 1=ANXIETY
  uint32_t sync_read_offset;
  uint32_t sync_mark_offset;
  uint8_t  sync_pending;
  uint8_t  sync_interrupted;
  float    threshold;
  uint8_t  sync_state;
  uint8_t  valid;              // 0xAA = written
  uint8_t  reserved[3];
};

// ─────────────────────────────────────────────────────────────────────────────
// Section 6: Global Variables
// ─────────────────────────────────────────────────────────────────────────────

// ── Mode ──────────────────────────────────────────────────────────────────────
uint8_t  currentMode           = MODE_TREMOR;
uint8_t  lastConnectedMode     = MODE_TREMOR;
float    current_ACCL_NOISE_THRESHOLD;

// ── IIR filter coefficients (8th order, must match exactly) ──────────────────
float b[9] = {0.00293679f, 0.0f, -0.01174716f, 0.0f,  0.01762074f,
              0.0f, -0.01174716f, 0.0f, 0.00293679f};
float a[9] = {1.0f, -6.14789409f, 16.90682328f, -27.19829387f, 28.01743143f,
              -18.93040648f, 8.1935523f, -2.07753259f, 0.23647338f};
float x_history[IMU_AXIS][IIR_ORDER] = {0};
float y_history[IMU_AXIS][IIR_ORDER] = {0};

// ── B1 flash write buffer ─────────────────────────────────────────────────────
uint8_t  b1_buffer[B1_BUFFER_SIZE];
uint16_t b1_fill        = 0;
uint16_t b1_flush_count = 0;

// ── B2 anxiety live BLE buffer ───────────────────────────────────────────────
uint8_t b2_buffer[B2_BUFFER_SIZE];
uint8_t b2_fill = 0;

// ── Flash tracking ────────────────────────────────────────────────────────────
const esp_partition_t* dataPartition         = NULL;
uint32_t flash_write_offset                  = FLASH_DATA_START;
uint32_t flash_bytes_written                 = 0;
bool     flash_recording                     = false;
bool     flash_full                          = false;
bool     flash_recording_paused              = false;
bool     flash_erase_in_progress             = false;
bool     flash_near_full_notified            = false;
uint16_t header_slot_index                   = 1;
uint16_t config_slot_index                   = 1;

// ── Sync state ────────────────────────────────────────────────────────────────
SyncState     syncState         = SYNC_IDLE;
uint32_t      sync_read_offset  = FLASH_DATA_START;
uint32_t      sync_mark_offset  = FLASH_DATA_START;
bool          sync_pending      = false;
bool          sync_interrupted  = false;
unsigned long lastSyncPktMs     = 0;
unsigned long t1_start_ms       = 0;

// ── BLE flags ─────────────────────────────────────────────────────────────────
volatile bool flashRefreshRequested = false;
volatile bool timeCharWritten       = false;
volatile bool wasConnected          = false;
char receivedTimeBuffer[21]         = {0};

// ── Algorithm state ───────────────────────────────────────────────────────────
bool          inTremorWindow    = false;
unsigned long tremorWindowStartMs = 0;

float Accel_Mag[MAX_DATA_POINTS];
float IMU_data[IMU_AXIS + 1];
float IMU_filtered[IMU_AXIS];
float IMU_magnitude[1]; // CHANNELS = 1

unsigned long realw_timestamps[MAX_DATA_POINTS];
int  dataIndex       = 0;
bool bufferFilled    = false;
int  tremor_count    = 0;
bool prev_tremor_state = false;
bool transmitBLE     = false;
unsigned long bleStartTime = 0;
int  tremor_data_count = 1;
unsigned long time_difference = 0;
unsigned long currentTime     = 0;
unsigned long lastReadTime    = 0;
int  crossingCount_Amag = 0;
bool Amag_tremor        = false;

// ── Display state ─────────────────────────────────────────────────────────────
volatile bool displayOn           = true;
volatile bool displayWakeRequested = false;
static uint32_t lastDisplayOnMs   = 0;
volatile bool forceRedraw         = false;
volatile bool pmuIRQ              = false;

static bool     touchWasDown        = false;
static uint32_t lastTapRegisteredMs = 0;
static bool     waitingSecondTap    = false;
static uint32_t firstTapMs          = 0;

String lastDisplayedTime = "";
String lastDisplayedDate = "";

// ── Semaphores ────────────────────────────────────────────────────────────────
SemaphoreHandle_t tftMutex          = NULL;
SemaphoreHandle_t sharedMutex       = NULL;
SemaphoreHandle_t displayStateMutex = NULL;
SemaphoreHandle_t rtcStateMutex     = NULL;

// ── RTC fallback state ────────────────────────────────────────────────────────
struct RTCFallbackState {
  bool isUsingRTC;
  int  referenceHour;
  int  referenceMinute;
  int  referenceDay;
  int  referenceMonth;
  int  referenceYear;
  unsigned long rtcModeStartMS;
  bool wasConnectedLastCheck;
};

RTCFallbackState rtcState = {
  false,
  0, 0,
  0, 0, 0,
  0,
  true
};

// ─────────────────────────────────────────────────────────────────────────────
// Section 7: Forward Declarations
// ─────────────────────────────────────────────────────────────────────────────
void flashInit();
void flashSaveOffsetHeader();
void flashSaveConfig();
void flashLoadConfig();
void flashStartRecording();
void flashStopRecording();
void flashFlushB1();
void b1AddRecord(float mag, uint16_t timeDiff, uint16_t rtcTime, uint8_t status);
void b1AddMarker(uint8_t h1, uint8_t h2, uint8_t* payload, uint8_t payloadLen);
void flashFlushPartialB1();
void imuSendMarker(uint8_t h1, uint8_t h2, uint8_t* payload, uint8_t payloadLen);
uint16_t flashGetRTCTime();
void b2TransferToFlash();
void flashAnxietyFullHandler();
void flashEraseTask(void* param);
void syncTrigger();
void syncSendChunk();
void syncStep();
void ensureFlashRecording();
void showFlashWarningOnTFT(const char* msg);
void calculateDisplayTimeWithRTCFallback(String& outTime, String& outDate);

// ─────────────────────────────────────────────────────────────────────────────
// Section 8: Mode String Helper
// ─────────────────────────────────────────────────────────────────────────────
static inline const char* getModeString() {
  return (currentMode == MODE_ANXIETY) ? "ANXIETY" : "TREMOR";
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 9: BLE Callbacks
// ─────────────────────────────────────────────────────────────────────────────
class MyServerCallbacks : public BLEServerCallbacks {
  void onConnect(BLEServer* pServer) {
    wasConnected = true;
    lastConnectedMode = currentMode;
    Serial.println("Central connected.");

    // Flush and save B1 state before switching paths
    flashStopRecording();
    Serial.println("FLASH: Stopped recording (BLE connected)");

    // TREMOR mode always records — restart immediately after stopping
    if (currentMode == MODE_TREMOR) {
      flashStartRecording();
    }

    // ANXIETY: if there is unsynced flash data, start a sync session
    if (currentMode == MODE_ANXIETY && sync_read_offset < flash_write_offset) {
      sync_mark_offset = flash_write_offset;
      sync_pending     = true;
      syncState        = SYNC_STARTING;
      flashSaveConfig();
      Serial.printf("SYNC: Anxiety on-connect — syncing %lu bytes\n",
                    sync_mark_offset - sync_read_offset);
    }

    // Interrupted sync resumes
    if (syncState == SYNC_INTERRUPTED) {
      sync_mark_offset = flash_write_offset;
      flashSaveConfig();
      syncState = SYNC_STARTING;
    }
  }

  void onDisconnect(BLEServer* pServer) {
    wasConnected = false;
    Serial.println("Central disconnected.");

    // Transfer any buffered B2 anxiety data to flash before going offline
    if (currentMode == MODE_ANXIETY && b2_fill > 0) {
      b2TransferToFlash();
    }

    // Resume offline flash recording
    ensureFlashRecording();

    BLEDevice::getAdvertising()->start();
  }
};

class TimeCharacteristicCallbacks : public BLECharacteristicCallbacks {
  void onWrite(BLECharacteristic* pCharacteristic) {
    std::string value = pCharacteristic->getValue();
    memset(receivedTimeBuffer, 0, sizeof(receivedTimeBuffer));
    memcpy(receivedTimeBuffer, value.data(),
           std::min((int)value.length(), 20));
    timeCharWritten = true;
  }
};

class FlashCmdCharacteristicCallbacks : public BLECharacteristicCallbacks {
  void onWrite(BLECharacteristic* pCharacteristic) {
    std::string value = pCharacteristic->getValue();
    if (value == "REFRESH") {
      Serial.println("FLASH-BLE: REFRESH command received");
      flashRefreshRequested = true;
    } else {
      Serial.printf("FLASH-BLE: Unknown command '%s'\n", value.c_str());
    }
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Section 10: UI Helper Functions (ported exactly from BLE26)
// ─────────────────────────────────────────────────────────────────────────────

static int16_t centerXForText(const char* txt, int textsize) {
  int len = strlen(txt);
  int w = len * 6 * textsize;
  return (240 - w) / 2;
}

static bool detectTap(uint32_t now) {
  bool touchDown = (digitalRead(TOUCH_INT) == LOW);
  bool tap = false;

  if (touchDown && !touchWasDown) {
    if (now - lastTapRegisteredMs >= TAP_DEBOUNCE_MS) {
      tap = true;
      lastTapRegisteredMs = now;
    }
  }
  touchWasDown = touchDown;
  return tap;
}

static void requestDisplayWake(uint32_t now) {
  if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
    displayWakeRequested = true;
    forceRedraw          = true;
    displayOn            = true;
    lastDisplayOnMs      = now;
    xSemaphoreGive(displayStateMutex);
  } else {
    displayWakeRequested = true;
    forceRedraw          = true;
    displayOn            = true;
    lastDisplayOnMs      = now;
  }
}

static void handleTap(uint32_t now) {
  if (displayOn) {
    if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
      lastDisplayOnMs = now;
      xSemaphoreGive(displayStateMutex);
    } else {
      lastDisplayOnMs = now;
    }
    waitingSecondTap = false;
    return;
  }

  if (!waitingSecondTap) {
    waitingSecondTap = true;
    firstTapMs       = now;
  } else {
    if (now - firstTapMs <= DOUBLE_TAP_GAP_MS) {
      requestDisplayWake(now);
    }
    waitingSecondTap = false;
  }
}

static void drawBluetoothIcon(int x, int y, bool connected) {
  if (connected) {
    tft->fillCircle(x, y, 11, 0x5DFF);
    tft->drawLine(x, y-7, x, y+7, TFT_BLACK);
    tft->drawLine(x, y-7, x+5, y-3, TFT_BLACK);
    tft->drawLine(x, y+7, x+5, y+3, TFT_BLACK);
    tft->drawLine(x+5, y-3, x, y,   TFT_BLACK);
    tft->drawLine(x+5, y+3, x, y,   TFT_BLACK);
  } else {
    tft->drawCircle(x, y, 11, 0x4208);
    tft->drawLine(x, y-7, x, y+7, 0x6B4D);
    tft->drawLine(x, y-7, x+5, y-3, 0x6B4D);
    tft->drawLine(x, y+7, x+5, y+3, 0x6B4D);
    tft->drawLine(x+5, y-3, x, y,   0x6B4D);
    tft->drawLine(x+5, y+3, x, y,   0x6B4D);
  }
}

static void drawBatteryIcon(int x_right, int y_top) {
  int pct      = 0;
  bool charging = false;

  if (watch->power) {
    pct      = watch->power->getBattPercentage();
    charging = watch->power->isChargeing();
  }
  if (pct < 0)   pct = 0;
  if (pct > 100) pct = 100;

  const int bodyW = 24, bodyH = 12;
  const int tipW  = 2,  tipH  = 6;
  int bodyX = x_right - bodyW;
  int bodyY = y_top + 4;

  tft->drawRoundRect(bodyX, bodyY, bodyW, bodyH, 2, TFT_WHITE);
  tft->fillRect(x_right + 1, bodyY + (bodyH - tipH)/2, tipW, tipH, TFT_WHITE);

  int innerW = bodyW - 4;
  int fillW  = (innerW * pct) / 100;
  uint16_t lvlColor = pct > 50 ? TFT_GREEN : (pct > 20 ? TFT_YELLOW : TFT_RED);
  if (fillW > 0) {
    tft->fillRect(bodyX + 2, bodyY + 2, fillW, bodyH - 4, lvlColor);
  }

  char pbuf[5];
  snprintf(pbuf, sizeof(pbuf), "%d", pct);
  tft->setTextSize(1);
  tft->setTextColor(TFT_WHITE, TFT_BLUE);
  int textW = strlen(pbuf) * 6;
  int textX = bodyX + (bodyW - textW) / 2;
  tft->setCursor(textX, bodyY + 2);
  tft->print(pbuf);
}

static void drawStatusBar() {
  tft->fillRect(0, 0, 240, 24, TFT_BLUE);
  tft->drawLine(0, 24, 240, 24, TFT_NAVY);

  tft->setTextSize(1);
  tft->setTextColor(TFT_WHITE, TFT_BLUE);
  tft->setCursor(4, 8);
  tft->print(getModeString());

  drawBluetoothIcon(225, 40, wasConnected);

  if (watch->power) {
    drawBatteryIcon(240 - 4, 2);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 11: Display Function (ported from BLE26, BLE28 indicators added)
// ─────────────────────────────────────────────────────────────────────────────

static void updateDisplay() {
  if (!displayOn) return;

  // Snapshot shared data
  String snapTime;
  String snapDate;
  String snapMode;
  int    snapCount;
  bool   snapWasConnected;
  SyncState snapSync;
  bool   snapFlashFull;
  bool   snapNearFull;
  bool   snapErasing;

  if (xSemaphoreTake(sharedMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
    snapTime         = lastDisplayedTime;
    snapDate         = lastDisplayedDate;
    snapMode         = String(getModeString());
    snapCount        = tremor_count;
    snapWasConnected = wasConnected;
    xSemaphoreGive(sharedMutex);
  } else {
    snapTime         = lastDisplayedTime;
    snapDate         = lastDisplayedDate;
    snapMode         = String(getModeString());
    snapCount        = tremor_count;
    snapWasConnected = wasConnected;
  }
  snapSync     = syncState;
  snapFlashFull = flash_full;
  snapNearFull  = flash_near_full_notified;
  snapErasing   = flash_erase_in_progress;

  static unsigned long lastUpdate = 0;
  static String lastTimeCache    = "";
  static String lastDateCache    = "";
  static int    lastCountCache   = -1;
  static String lastModeCache    = "";
  static bool   lastBLECache     = false;
  static SyncState lastSyncCache = SYNC_IDLE;
  static bool   lastFullCache    = false;
  static bool   lastNearCache    = false;
  static bool   lastEraseCache   = false;

  unsigned long currentMillis = millis();
  bool timeToUpdate = (currentMillis - lastUpdate >= 60000);

  bool dataChanged =
    (snapTime         != lastTimeCache)   ||
    (snapDate         != lastDateCache)   ||
    (snapCount        != lastCountCache)  ||
    (snapMode         != lastModeCache)   ||
    (snapWasConnected != lastBLECache)    ||
    (snapSync         != lastSyncCache)   ||
    (snapFlashFull    != lastFullCache)   ||
    (snapNearFull     != lastNearCache)   ||
    (snapErasing      != lastEraseCache);

  if (!forceRedraw && !timeToUpdate && !dataChanged) return;

  lastUpdate     = currentMillis;
  lastTimeCache  = snapTime;
  lastDateCache  = snapDate;
  lastCountCache = snapCount;
  lastModeCache  = snapMode;
  lastBLECache   = snapWasConnected;
  lastSyncCache  = snapSync;
  lastFullCache  = snapFlashFull;
  lastNearCache  = snapNearFull;
  lastEraseCache = snapErasing;

  if (xSemaphoreTake(tftMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    tft->fillScreen(TFT_BLACK);

    drawStatusBar();

    // Title
    tft->setTextColor(TFT_BLUE, TFT_BLACK);
    tft->setTextSize(2);
    int16_t x = centerXForText("iGest", 2);
    tft->setCursor(x, 40);
    tft->print("iGest");

    // Passcode
    tft->setTextColor(TFT_WHITE, TFT_BLACK);
    tft->setTextSize(2);
    x = centerXForText("6373", 2);
    tft->setCursor(x, 70);
    tft->print("6373");

    // RTC fallback for time/date
    String displayTime = snapTime;
    String displayDate = snapDate;
    if (!snapWasConnected) {
      calculateDisplayTimeWithRTCFallback(displayTime, displayDate);
    }

    if (displayTime.length() > 0) {
      tft->setTextSize(3);
      tft->setTextColor(TFT_WHITE, TFT_BLACK);
      char timeBuf[32];
      displayTime.toCharArray(timeBuf, sizeof(timeBuf));
      x = centerXForText(timeBuf, 3);
      tft->setCursor(x, 112);
      tft->print(displayTime);

      if (displayDate.length() > 0) {
        tft->setTextSize(2);
        tft->setTextColor(TFT_DARKGREY, TFT_BLACK);
        char dateBuf[32];
        displayDate.toCharArray(dateBuf, sizeof(dateBuf));
        int16_t dx = centerXForText(dateBuf, 2);
        tft->setCursor(dx, 145);
        tft->print(displayDate);
      }
    }

    // Tremor count (only shown when BLE connected)
    if (snapWasConnected) {
      char countStr[20];
      snprintf(countStr, sizeof(countStr), "Count: %d", snapCount);
      tft->setTextSize(2);
      tft->setTextColor(TFT_WHITE, TFT_BLACK);
      x = centerXForText(countStr, 2);
      tft->setCursor(x, 190);
      tft->print(countStr);
    }

    // ── BLE28 state indicators ──
    if (snapSync == SYNC_ACTIVE) {
      tft->setTextSize(2);
      tft->setTextColor(TFT_CYAN, TFT_BLACK);
      x = centerXForText("SYNCING", 2);
      tft->setCursor(x, 215);
      tft->print("SYNCING");
    } else if (snapErasing) {
      tft->setTextSize(2);
      tft->setTextColor(TFT_ORANGE, TFT_BLACK);
      x = centerXForText("RESETTING...", 2);
      tft->setCursor(x, 215);
      tft->print("RESETTING...");
    } else if (snapFlashFull && currentMode == MODE_TREMOR) {
      tft->setTextSize(2);
      tft->setTextColor(TFT_RED, TFT_BLACK);
      x = centerXForText("STORAGE FULL", 2);
      tft->setCursor(x, 215);
      tft->print("STORAGE FULL");
    } else if (snapNearFull && currentMode == MODE_TREMOR) {
      tft->setTextSize(2);
      tft->setTextColor(TFT_YELLOW, TFT_BLACK);
      x = centerXForText("STORAGE 90%", 2);
      tft->setCursor(x, 215);
      tft->print("STORAGE 90%");
    }

    if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
      forceRedraw = false;
      xSemaphoreGive(displayStateMutex);
    } else {
      forceRedraw = false;
    }

    xSemaphoreGive(tftMutex);
  }
}

void displayTask(void *pvParameters) {
  bool blState = true;

  for (;;) {
    bool wakeReq = false;
    bool dispOn  = false;

    if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
      wakeReq = displayWakeRequested;
      dispOn  = displayOn;
      xSemaphoreGive(displayStateMutex);
    } else {
      wakeReq = displayWakeRequested;
      dispOn  = displayOn;
    }

    if (wakeReq) {
      if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
        displayWakeRequested = false;
        displayOn = true;
        xSemaphoreGive(displayStateMutex);
      } else {
        displayWakeRequested = false;
        displayOn = true;
      }

      if (!blState) { watch->openBL(); blState = true; }
      updateDisplay();
      vTaskDelay(pdMS_TO_TICKS(200));
      continue;
    }

    if (dispOn) {
      if (!blState) { watch->openBL(); blState = true; }
      updateDisplay();
      vTaskDelay(pdMS_TO_TICKS(1000));
    } else {
      if (blState) { watch->closeBL(); blState = false; }
      vTaskDelay(pdMS_TO_TICKS(500));
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 12: Detection Algorithm (ported exactly from BLE26)
// ─────────────────────────────────────────────────────────────────────────────

float calculateBaseline(float dataArray[], unsigned long ct) {
  float sum = 0;
  int   baseline_count = 0;
  unsigned long startTime = ct - (unsigned long)(BASELINE_TIMEFRAME * 1000.0f);

  int index = dataIndex - 1;
  if (index < 0) index = MAX_DATA_POINTS - 1;

  for (int i = 0; i < MAX_DATA_POINTS; i++) {
    if (realw_timestamps[index] >= startTime) {
      sum += dataArray[index];
      baseline_count++;
    } else {
      break;
    }
    index--;
    if (index < 0) index = MAX_DATA_POINTS - 1;
  }
  return (baseline_count > 0) ? (sum / baseline_count) : 0;
}

int countCrossings(float dataArray[], float baseline, unsigned long ct,
                   float threshold) {
  int  crossings = 0;
  char prevState = 'N';
  unsigned long startTime = ct - (unsigned long)(CROSSING_TIMEFRAME * 1000.0f);

  int index = dataIndex - 1;
  if (index < 0) index = MAX_DATA_POINTS - 1;

  for (int i = 0; i < MAX_DATA_POINTS; i++) {
    if (realw_timestamps[index] >= startTime) {
      float value = dataArray[index];
      char  currentState;
      if      (value > baseline + threshold) currentState = 'A';
      else if (value < baseline - threshold) currentState = 'B';
      else                                   currentState = 'N';

      if ((prevState == 'A' && currentState == 'B') ||
          (prevState == 'B' && currentState == 'A')) {
        crossings++;
      }
      if (currentState != 'N') prevState = currentState;
    } else {
      break;
    }
    index--;
    if (index < 0) index = MAX_DATA_POINTS - 1;
  }
  return crossings;
}

void calculateMagnitudes() {
  IMU_magnitude[0] = sqrt(
    IMU_filtered[0] * IMU_filtered[0] +
    IMU_filtered[1] * IMU_filtered[1] +
    IMU_filtered[2] * IMU_filtered[2]
  );
}

float applyIIRFilter(float x, int index) {
  float y = b[0] * x;
  for (int i = 0; i < IIR_ORDER; i++) {
    y += b[i + 1] * x_history[index][i];
    y -= a[i + 1] * y_history[index][i];
  }
  for (int i = IIR_ORDER - 1; i > 0; i--) {
    x_history[index][i] = x_history[index][i - 1];
    y_history[index][i] = y_history[index][i - 1];
  }
  x_history[index][0] = x;
  y_history[index][0] = y;
  return y;
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 13: RTC Fallback Helpers (ported exactly from BLE26)
// ─────────────────────────────────────────────────────────────────────────────

void syncRTCWithTime(int hh, int mm, int ss, int dd, int mon, int yyyy) {
  if (watch && watch->rtc) {
    watch->rtc->setDateTime(yyyy, mon, dd, hh, mm, ss);
    Serial.printf("RTC: Hardware synced to %02d:%02d:%02d on %02d-%02d-%04d\n",
                  hh, mm, ss, dd, mon, yyyy);
  }
}

void updateRTCReference(int hh, int mm, int ss, int dd, int mon, int yyyy) {
  if (xSemaphoreTake(rtcStateMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
    rtcState.referenceHour   = hh;
    rtcState.referenceMinute = mm;
    rtcState.referenceDay    = dd;
    rtcState.referenceMonth  = mon;
    rtcState.referenceYear   = yyyy;
    xSemaphoreGive(rtcStateMutex);
  }
  syncRTCWithTime(hh, mm, ss, dd, mon, yyyy);
}

void monitorConnectionStateForRTC(bool currentConnection) {
  if (xSemaphoreTake(rtcStateMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
    if (rtcState.wasConnectedLastCheck && !currentConnection) {
      Serial.println("RTC: BLE disconnected — switching to RTC fallback mode");
      rtcState.isUsingRTC      = true;
      rtcState.rtcModeStartMS  = millis();
    }
    if (!rtcState.wasConnectedLastCheck && currentConnection) {
      Serial.println("RTC: BLE reconnected — switching back to BLE time mode");
      rtcState.isUsingRTC = false;
    }
    rtcState.wasConnectedLastCheck = currentConnection;
    xSemaphoreGive(rtcStateMutex);
  }
}

void calculateDisplayTimeWithRTCFallback(String& outTime, String& outDate) {
  if (xSemaphoreTake(rtcStateMutex, pdMS_TO_TICKS(50)) != pdTRUE) return;

  if (!rtcState.isUsingRTC) {
    xSemaphoreGive(rtcStateMutex);
    return;
  }

  unsigned long elapsedMS  = millis() - rtcState.rtcModeStartMS;
  unsigned long elapsedMin = elapsedMS / 60000UL;

  int year     = rtcState.referenceYear;
  int month    = rtcState.referenceMonth;
  int day      = rtcState.referenceDay;
  int refHour  = rtcState.referenceHour;
  int refMinute= rtcState.referenceMinute;

  unsigned long totalMinutes = (unsigned long)refHour * 60UL +
                               (unsigned long)refMinute + elapsedMin;
  unsigned long daysToAdd    = totalMinutes / (24UL * 60UL);

  int dispHH = refHour;
  int dispMM = refMinute;

  if (watch && watch->rtc) {
    const char* rtcStr = watch->rtc->formatDateTime();
    if (rtcStr && strlen(rtcStr) >= 5 && rtcStr[2] == ':') {
      int h = (isDigit(rtcStr[0]) && isDigit(rtcStr[1])) ?
              (rtcStr[0]-'0')*10 + (rtcStr[1]-'0') : -1;
      int m = (isDigit(rtcStr[3]) && isDigit(rtcStr[4])) ?
              (rtcStr[3]-'0')*10 + (rtcStr[4]-'0') : -1;
      if (h >= 0 && h <= 23 && m >= 0 && m <= 59) {
        dispHH = h; dispMM = m;
      }
    }
  }

  char timeBuf[6];
  snprintf(timeBuf, sizeof(timeBuf), "%02d:%02d", dispHH, dispMM);
  outTime = String(timeBuf);

  auto isLeapYear = [](int y) -> bool {
    return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));
  };
  auto daysInMonth = [&](int y, int m) -> int {
    static const int dim[13] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (m == 2) return isLeapYear(y) ? 29 : 28;
    if (m >= 1 && m <= 12) return dim[m];
    return 31;
  };

  while (daysToAdd > 0) {
    int dim2 = daysInMonth(year, month);
    int remainingInMonth = dim2 - day;
    if (daysToAdd <= (unsigned long)remainingInMonth) {
      day += (int)daysToAdd;
      daysToAdd = 0;
    } else {
      daysToAdd -= (unsigned long)(remainingInMonth + 1);
      day = 1;
      month++;
      if (month > 12) { month = 1; year++; }
    }
  }

  char dateBuf[11];
  snprintf(dateBuf, sizeof(dateBuf), "%02d-%02d-%04d", day, month, year);
  outDate = String(dateBuf);

  xSemaphoreGive(rtcStateMutex);
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 14: Flash Manager
// ─────────────────────────────────────────────────────────────────────────────

uint16_t flashGetRTCTime() {
  uint8_t hh = 0, mm = 0;
  if (watch && watch->rtc) {
    const char* rtcStr = watch->rtc->formatDateTime();
    if (rtcStr && strlen(rtcStr) >= 5 && rtcStr[2] == ':') {
      if (isDigit(rtcStr[0]) && isDigit(rtcStr[1]))
        hh = (rtcStr[0]-'0')*10 + (rtcStr[1]-'0');
      if (isDigit(rtcStr[3]) && isDigit(rtcStr[4]))
        mm = (rtcStr[3]-'0')*10 + (rtcStr[4]-'0');
    }
  }
  return (uint16_t)(hh * 100 + mm);
}

void showFlashWarningOnTFT(const char* msg) {
  if (!tft) return;
  if (xSemaphoreTake(tftMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    tft->setTextSize(2);
    tft->setTextColor(TFT_RED, TFT_BLACK);
    int16_t x = centerXForText(msg, 2);
    tft->setCursor(x, 215);
    tft->print(msg);
    xSemaphoreGive(tftMutex);
  }
  forceRedraw = true;
}

// Shared full-sector-0 refresh — called when either slot index overflows.
// Erases sector 0, rewrites both magics, and resets both indices to 1.
// IMPORTANT: call this before writing, with no recursive save calls.
static void flashRefreshSector0WithState() {
  if (!dataPartition) return;

  esp_partition_erase_range(dataPartition, 0, FLASH_SECTOR_SIZE);

  uint32_t m1 = FLASH_MAGIC;
  esp_partition_write(dataPartition, FLASH_OFFSET_HDR_START, &m1, 4);

  uint32_t m2 = FLASH_CONFIG_MAGIC;
  esp_partition_write(dataPartition, FLASH_CONFIG_HDR_START, &m2, 4);

  header_slot_index = 1;
  config_slot_index = 1;

  // Immediately rewrite current offset and config in slot 1
  uint32_t slot_off = FLASH_OFFSET_HDR_START + header_slot_index * 4;
  esp_partition_write(dataPartition, slot_off, &flash_write_offset, 4);
  header_slot_index++;

  ConfigSlot cs;
  cs.last_mode       = currentMode;
  cs.sync_read_offset   = sync_read_offset;
  cs.sync_mark_offset   = sync_mark_offset;
  cs.sync_pending    = sync_pending    ? 1 : 0;
  cs.sync_interrupted= sync_interrupted? 1 : 0;
  cs.threshold       = current_ACCL_NOISE_THRESHOLD;
  cs.sync_state      = (uint8_t)syncState;
  cs.valid           = 0xAA;
  memset(cs.reserved, 0, 3);

  uint32_t cfg_off = FLASH_CONFIG_HDR_START + 4 + (config_slot_index - 1) * 20;
  esp_partition_write(dataPartition, cfg_off, &cs, sizeof(ConfigSlot));
  config_slot_index++;

  Serial.println("FLASH: Sector-0 refresh complete");
}

void flashSaveOffsetHeader() {
  if (!dataPartition) return;

  if (header_slot_index >= MAX_HEADER_SLOTS) {
    flashRefreshSector0WithState();
    return; // both slots already written by flashRefreshSector0WithState
  }

  uint32_t slot_off = FLASH_OFFSET_HDR_START + header_slot_index * 4;
  esp_partition_write(dataPartition, slot_off, &flash_write_offset, 4);
  header_slot_index++;
}

void flashSaveConfig() {
  if (!dataPartition) return;

  if (config_slot_index >= MAX_CONFIG_SLOTS) {
    flashRefreshSector0WithState();
    return; // config already written by flashRefreshSector0WithState
  }

  ConfigSlot cs;
  cs.last_mode        = currentMode;
  cs.sync_read_offset    = sync_read_offset;
  cs.sync_mark_offset    = sync_mark_offset;
  cs.sync_pending     = sync_pending     ? 1 : 0;
  cs.sync_interrupted = sync_interrupted ? 1 : 0;
  cs.threshold        = current_ACCL_NOISE_THRESHOLD;
  cs.sync_state       = (uint8_t)syncState;
  cs.valid            = 0xAA;
  memset(cs.reserved, 0, 3);

  uint32_t cfg_off = FLASH_CONFIG_HDR_START + 4 + (config_slot_index - 1) * 20;
  esp_partition_write(dataPartition, cfg_off, &cs, sizeof(ConfigSlot));
  config_slot_index++;
}

void flashLoadConfig() {
  if (!dataPartition) return;

  uint32_t cfgMagic = 0;
  esp_partition_read(dataPartition, FLASH_CONFIG_HDR_START, &cfgMagic, 4);
  if (cfgMagic != FLASH_CONFIG_MAGIC) return;

  ConfigSlot best;
  best.valid = 0;
  uint16_t bestIdx = 0;

  for (uint16_t i = 1; i <= MAX_CONFIG_SLOTS; i++) {
    ConfigSlot cs;
    uint32_t off = FLASH_CONFIG_HDR_START + 4 + (i - 1) * 20;
    esp_partition_read(dataPartition, off, &cs, sizeof(ConfigSlot));
    if (cs.valid == 0xAA) {
      best    = cs;
      bestIdx = i;
    } else {
      break; // slots are written sequentially; first empty = end
    }
  }

  if (best.valid != 0xAA) return;

  config_slot_index = bestIdx + 1;

  currentMode   = best.last_mode;
  sync_read_offset    = best.sync_read_offset;
  sync_mark_offset    = best.sync_mark_offset;
  sync_pending        = (best.sync_pending    != 0);
  sync_interrupted    = (best.sync_interrupted!= 0);
  current_ACCL_NOISE_THRESHOLD = best.threshold;
  syncState           = (SyncState)best.sync_state;
  lastConnectedMode   = currentMode;

  // Validate sync offsets
  if (sync_read_offset < FLASH_DATA_START)
    sync_read_offset = FLASH_DATA_START;
  if (sync_mark_offset < FLASH_DATA_START)
    sync_mark_offset = FLASH_DATA_START;

  Serial.printf("CONFIG: Loaded — mode=%u  sync_read=0x%lX  syncState=%u\n",
                currentMode, sync_read_offset, (uint8_t)syncState);
}

void flashInit() {
  dataPartition = esp_partition_find_first(
    ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_DATA_SPIFFS, NULL);
  if (!dataPartition) {
    dataPartition = esp_partition_find_first(
      ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY, "spiffs");
  }
  if (!dataPartition) {
    Serial.println("FLASH: No partition found! Recording disabled.");
    flash_full = true;
    return;
  }
  Serial.printf("FLASH: Partition '%s' @ 0x%X, size=%lu bytes\n",
                dataPartition->label, dataPartition->address,
                dataPartition->size);

  // Read offset header magic
  uint32_t magic = 0;
  esp_partition_read(dataPartition, FLASH_OFFSET_HDR_START, &magic, 4);

  if (magic == FLASH_MAGIC) {
    // Scan offset slots 1–511
    uint32_t last_valid_offset = FLASH_DATA_START;
    header_slot_index = 1;
    for (uint16_t i = 1; i <= MAX_HEADER_SLOTS; i++) {
      uint32_t val = 0;
      esp_partition_read(dataPartition,
                         FLASH_OFFSET_HDR_START + i * 4, &val, 4);
      if (val == 0xFFFFFFFF) break;
      last_valid_offset = val;
      header_slot_index = i + 1;
    }

    if (last_valid_offset >= FLASH_DATA_START &&
        last_valid_offset < dataPartition->size) {
      flash_write_offset  = last_valid_offset;
      flash_bytes_written = last_valid_offset - FLASH_DATA_START;
      Serial.printf("FLASH: Existing data — %lu bytes (slot %u)\n",
                    flash_bytes_written, header_slot_index - 1);
    } else {
      flash_write_offset  = FLASH_DATA_START;
      flash_bytes_written = 0;
    }

    // Load config from Half 2
    flashLoadConfig();

    // Resume sync if interrupted
    if (sync_interrupted && syncState == SYNC_INTERRUPTED) {
      Serial.println("SYNC: Resuming interrupted sync on boot");
    }

  } else {
    // Fresh start — erase sector 0, write both magics
    esp_partition_erase_range(dataPartition, 0, FLASH_SECTOR_SIZE);
    uint32_t m1 = FLASH_MAGIC;
    esp_partition_write(dataPartition, FLASH_OFFSET_HDR_START, &m1, 4);
    uint32_t m2 = FLASH_CONFIG_MAGIC;
    esp_partition_write(dataPartition, FLASH_CONFIG_HDR_START, &m2, 4);
    flash_write_offset  = FLASH_DATA_START;
    flash_bytes_written = 0;
    header_slot_index   = 1;
    config_slot_index   = 1;
    sync_read_offset    = FLASH_DATA_START;
    sync_mark_offset    = FLASH_DATA_START;
    Serial.println("FLASH: No valid header — starting fresh");
  }

  if (flash_bytes_written >= FLASH_MAX_BYTES) {
    flash_full = true;
    Serial.println("FLASH: Full on boot");
    return;
  }

  // Pre-erase unused sectors ahead of write offset
  // (one-time ~51 second penalty; sampling not yet running)
  uint32_t erase_start =
    ((flash_write_offset / FLASH_SECTOR_SIZE) + 1) * FLASH_SECTOR_SIZE;

  if (erase_start < dataPartition->size) {
    uint32_t erase_size = dataPartition->size - erase_start;
    erase_size = (erase_size / FLASH_SECTOR_SIZE) * FLASH_SECTOR_SIZE;

    if (erase_size > 0) {
      // Show progress on TFT (tft is already initialised before flashInit)
      if (tft && xSemaphoreTake(tftMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        tft->fillScreen(TFT_BLACK);
        tft->setTextSize(2);
        tft->setTextColor(TFT_YELLOW, TFT_BLACK);
        int16_t ex = centerXForText("Erasing...", 2);
        tft->setCursor(ex, 110);
        tft->print("Erasing...");
        xSemaphoreGive(tftMutex);
      }

      Serial.printf("FLASH: Pre-erasing %lu bytes (%lu sectors)...\n",
                    erase_size, erase_size / FLASH_SECTOR_SIZE);
      unsigned long t0 = millis();
      esp_partition_erase_range(dataPartition, erase_start, erase_size);
      Serial.printf("FLASH: Pre-erase done in %lu ms\n", millis() - t0);
    }
  }

  Serial.printf("FLASH: Ready — write_offset=0x%lX  %lu bytes used\n",
                flash_write_offset, flash_bytes_written);
}

void flashStartRecording() {
  if (flash_full || flash_recording_paused || flash_erase_in_progress ||
      !dataPartition) {
    return;
  }
  flash_recording = true;
  b1_fill = 0;
  Serial.printf("FLASH: Recording started (offset=0x%lX)\n", flash_write_offset);
}

void flashStopRecording() {
  if (!flash_recording) return;
  flashFlushPartialB1();
  flashSaveOffsetHeader();
  flashSaveConfig();
  flash_recording = false;
  Serial.printf("FLASH: Recording stopped (offset=0x%lX, %lu bytes)\n",
                flash_write_offset, flash_bytes_written);
}

// Pure page-program write — partition must already be erased
void flashFlushB1() {
  if (b1_fill == 0 || !dataPartition) return;
  if (flash_full || flash_recording_paused || flash_erase_in_progress) return;

  uint32_t bytes_to_write = (uint32_t)b1_fill * B1_RECORD_SIZE;

  // Check FLASH_MAX_BYTES limit
  if (flash_bytes_written + bytes_to_write > FLASH_MAX_BYTES) {
    uint32_t remaining    = FLASH_MAX_BYTES - flash_bytes_written;
    uint32_t samples_fit  = remaining / B1_RECORD_SIZE;
    bytes_to_write        = samples_fit * B1_RECORD_SIZE;

    if (bytes_to_write == 0) {
      b1_fill = 0;
      if (currentMode == MODE_TREMOR) {
        flash_full             = true;
        flash_recording_paused = true;
        flash_recording        = false;
        Serial.println("FLASH: TREMOR 100% — recording paused");
      } else {
        flashAnxietyFullHandler();
      }
      return;
    }
  }

  // Check partition boundary
  if (flash_write_offset + bytes_to_write > dataPartition->size) {
    b1_fill = 0;
    flash_full = true;
    if (currentMode == MODE_TREMOR) {
      flash_recording_paused = true;
      flash_recording        = false;
    } else {
      flashAnxietyFullHandler();
    }
    return;
  }

  esp_err_t err = esp_partition_write(dataPartition, flash_write_offset,
                                      b1_buffer, bytes_to_write);
  if (err != ESP_OK) {
    Serial.printf("FLASH: Write error %d at 0x%lX\n", err, flash_write_offset);
  }

  flash_write_offset  += bytes_to_write;
  flash_bytes_written += bytes_to_write;
  b1_fill              = 0;
  b1_flush_count++;

  // Save offset header every 10 flushes (~30 seconds at 100 Hz)
  if (b1_flush_count >= 10) {
    flashSaveOffsetHeader();
    b1_flush_count = 0;
  }

  // TREMOR 90% warning
  if (currentMode == MODE_TREMOR &&
      flash_bytes_written >= FLASH_WARN_THRESHOLD &&
      !flash_near_full_notified) {
    flash_near_full_notified = true;
    forceRedraw = true;
    Serial.println("FLASH: TREMOR 90% warning");
  }

  // TREMOR 100% pause
  if (currentMode == MODE_TREMOR &&
      flash_bytes_written >= FLASH_MAX_BYTES &&
      !flash_recording_paused) {
    flash_full             = true;
    flash_recording_paused = true;
    flash_recording        = false;
    forceRedraw = true;
    Serial.println("FLASH: TREMOR 100% — recording paused");
  }
}

void b1AddRecord(float mag, uint16_t timeDiff, uint16_t rtcTime,
                 uint8_t status) {
  if (flash_recording_paused || flash_full || flash_erase_in_progress ||
      !dataPartition) return;
  if (!flash_recording) flashStartRecording();

  uint16_t off = b1_fill * B1_RECORD_SIZE;
  memcpy(&b1_buffer[off + 0], &mag,      4);
  memcpy(&b1_buffer[off + 4], &timeDiff, 2);
  memcpy(&b1_buffer[off + 6], &rtcTime,  2);
  b1_buffer[off + 8] = status;
  b1_fill++;

  if (b1_fill >= B1_SAMPLES) flashFlushB1();
}

void b1AddMarker(uint8_t h1, uint8_t h2, uint8_t* payload,
                 uint8_t payloadLen) {
  if (flash_recording_paused || flash_full || flash_erase_in_progress ||
      !dataPartition) return;
  if (!flash_recording) flashStartRecording();

  uint16_t off = b1_fill * B1_RECORD_SIZE;
  memset(&b1_buffer[off], 0, B1_RECORD_SIZE);
  b1_buffer[off + 0] = h1;
  b1_buffer[off + 1] = h2;
  if (payload && payloadLen > 0) {
    uint8_t maxPay = B1_RECORD_SIZE - 2;
    if (payloadLen > maxPay) payloadLen = maxPay;
    memcpy(&b1_buffer[off + 2], payload, payloadLen);
  }
  b1_fill++;

  if (b1_fill >= B1_SAMPLES) flashFlushB1();
}

void flashFlushPartialB1() {
  if (b1_fill > 0 && b1_fill < B1_SAMPLES) {
    uint16_t off = b1_fill * B1_RECORD_SIZE;
    uint16_t rem = (B1_SAMPLES - b1_fill) * B1_RECORD_SIZE;
    memset(&b1_buffer[off], 0, rem);
    b1_fill = B1_SAMPLES;
    flashFlushB1();
  }
}

void ensureFlashRecording() {
  if (!flash_recording && !flash_full && !flash_recording_paused &&
      !flash_erase_in_progress && dataPartition) {
    flashStartRecording();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 15: B2 Buffer & Anxiety Live Stream
// ─────────────────────────────────────────────────────────────────────────────

void b2AddRecord(float mag, uint16_t timeDiff, uint8_t status) {
  if (b2_fill >= B2_SAMPLES) return;
  uint16_t off = b2_fill * B2_RECORD_SIZE;
  memcpy(&b2_buffer[off + 0], &mag,      4);
  memcpy(&b2_buffer[off + 4], &timeDiff, 2);
  b2_buffer[off + 6] = status;
  b2_fill++;
}

void b2SendPacket() {
  imuCharacteristic->setValue(b2_buffer, B2_BUFFER_SIZE);
  imuCharacteristic->notify();
  b2_fill = 0;
}

// Send a marker packet over the IMU characteristic (live anxiety path).
// Format mirrors the 9-byte B1 record: [h1][h2][payload 0..6], zero-padded.
void imuSendMarker(uint8_t h1, uint8_t h2, uint8_t* payload,
                   uint8_t payloadLen) {
  uint8_t markerBuf[B1_RECORD_SIZE];
  memset(markerBuf, 0, B1_RECORD_SIZE);
  markerBuf[0] = h1;
  markerBuf[1] = h2;
  if (payload && payloadLen > 0) {
    uint8_t maxPay = B1_RECORD_SIZE - 2;
    if (payloadLen > maxPay) payloadLen = maxPay;
    memcpy(&markerBuf[2], payload, payloadLen);
  }
  imuCharacteristic->setValue(markerBuf, B1_RECORD_SIZE);
  imuCharacteristic->notify();
}

// Transfer B2 anxiety buffer to flash on disconnect (Phase 5)
void b2TransferToFlash() {
  if (b2_fill == 0) return;
  ensureFlashRecording();

  // B2 START marker
  b1AddMarker(MARKER_B2_START_H1, MARKER_B2_START_H2, NULL, 0);

  uint16_t rtcNow = flashGetRTCTime();
  for (uint8_t i = 0; i < b2_fill; i++) {
    uint16_t off7 = i * B2_RECORD_SIZE;
    float    mag;
    uint16_t timeDiff;
    uint8_t  status;
    memcpy(&mag,      &b2_buffer[off7 + 0], 4);
    memcpy(&timeDiff, &b2_buffer[off7 + 4], 2);
    status = b2_buffer[off7 + 6];
    b1AddRecord(mag, timeDiff, rtcNow, status);
  }

  // B2 END marker
  b1AddMarker(MARKER_B2_END_H1, MARKER_B2_END_H2, NULL, 0);

  b2_fill = 0;
  Serial.println("B2->B1: Transfer complete");
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 16: Flash Full Handlers (Phase 8)
// ─────────────────────────────────────────────────────────────────────────────

void flashEraseTask(void* param) {
  for (uint32_t off = FLASH_DATA_START;
       off < dataPartition->size;
       off += FLASH_SECTOR_SIZE) {
    esp_partition_erase_range(dataPartition, off, FLASH_SECTOR_SIZE);
    vTaskDelay(pdMS_TO_TICKS(5)); // yield so loop() keeps sampling at 100 Hz
  }

  flash_write_offset       = FLASH_DATA_START;
  flash_bytes_written      = 0;
  sync_read_offset         = FLASH_DATA_START;
  sync_mark_offset         = FLASH_DATA_START;
  flash_erase_in_progress  = false;
  flash_near_full_notified = false;
  flash_full               = false;
  flash_recording_paused   = false;
  flash_recording          = true;
  b1_fill                  = 0;
  flashSaveConfig();
  forceRedraw = true;
  Serial.println("FLASH: Background erase complete — recording resumed");
  vTaskDelete(NULL);
}

void flashAnxietyFullHandler() {
  flash_recording     = false;
  flash_erase_in_progress = true;
  b1_fill = 0;
  showFlashWarningOnTFT("STORAGE FULL");
  Serial.println("FLASH: ANXIETY full — starting background erase");
  xTaskCreatePinnedToCore(flashEraseTask, "flashErase",
                          4096, NULL, 1, NULL, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 17: Sync State Machine (Phase 7)
// ─────────────────────────────────────────────────────────────────────────────

void syncTrigger() {
  sync_pending = true;
  if (wasConnected) {
    sync_mark_offset = flash_write_offset;
    syncState        = SYNC_STARTING;
  } else {
    sync_mark_offset = flash_write_offset;
    syncState        = SYNC_WAITING_BLE;
  }
  flashSaveConfig();
  Serial.printf("SYNC: Triggered — mark=0x%lX  state=%u\n",
                sync_mark_offset, (uint8_t)syncState);
}

void syncSendChunk() {
  if (!dataPartition || !wasConnected) return;
  if (sync_read_offset >= sync_mark_offset) return;

  uint32_t bytes_left = sync_mark_offset - sync_read_offset;
  uint16_t chunk_size = (bytes_left >= B3_PKT_SIZE) ?
                        (uint16_t)B3_PKT_SIZE :
                        (uint16_t)bytes_left;

  static uint8_t syncPkt[B3_PKT_SIZE];
  esp_err_t err = esp_partition_read(dataPartition, sync_read_offset,
                                     syncPkt, chunk_size);
  if (err != ESP_OK) {
    Serial.printf("SYNC: Read error %d at 0x%lX\n", err, sync_read_offset);
    return;
  }

  flashDataCharacteristic->setValue(syncPkt, chunk_size);
  flashDataCharacteristic->notify();
  sync_read_offset += chunk_size;
  flashSaveConfig();
}

void syncStep() {
  unsigned long now = millis();

  switch (syncState) {

    case SYNC_IDLE:
      // Tremor T1 timer
      if (currentMode == MODE_TREMOR &&
          (now - t1_start_ms) >= T1_INTERVAL_MS) {
        t1_start_ms = now; // reset T1 immediately
        syncTrigger();
      }
      break;

    case SYNC_WAITING_BLE:
      if (wasConnected) {
        sync_mark_offset = flash_write_offset; // extend to latest
        flashSaveConfig();
        syncState = SYNC_STARTING;
      }
      break;

    case SYNC_STARTING: {
      if (!wasConnected) { syncState = SYNC_INTERRUPTED; break; }
      flashDataCharacteristic->setValue((uint8_t*)SYNC_BLE_START, 4);
      flashDataCharacteristic->notify();
      uint8_t tb[4];
      memcpy(tb, &sync_mark_offset, 4);
      flashDataCharacteristic->setValue(tb, 4);
      flashDataCharacteristic->notify();
      lastSyncPktMs = millis();
      syncState = SYNC_ACTIVE;
      forceRedraw = true;
      Serial.printf("SYNC: Started — sending 0x%lX to 0x%lX\n",
                    sync_read_offset, sync_mark_offset);
      break;
    }

    case SYNC_ACTIVE:
      if (!wasConnected) {
        flashSaveConfig();
        syncState = SYNC_INTERRUPTED;
        forceRedraw = true;
        break;
      }
      if (now - lastSyncPktMs < T2_SYNC_INTERVAL_MS) break;
      if (sync_read_offset >= sync_mark_offset) {
        flashDataCharacteristic->setValue((uint8_t*)SYNC_BLE_END, 4);
        flashDataCharacteristic->notify();
        syncState = SYNC_COMPLETE;
        break;
      }
      syncSendChunk();
      lastSyncPktMs = now;
      break;

    case SYNC_INTERRUPTED:
      if (wasConnected) {
        sync_mark_offset = flash_write_offset; // extend to latest
        flashSaveConfig();
        syncState = SYNC_STARTING;
      }
      break;

    case SYNC_COMPLETE:
      sync_pending     = false;
      sync_interrupted = false;
      flashSaveConfig();
      syncState   = SYNC_IDLE;
      forceRedraw = true;
      Serial.printf("SYNC: Complete — sync_read=0x%lX\n", sync_read_offset);
      break;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 18: setup()
// ─────────────────────────────────────────────────────────────────────────────

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("iGest v3 BLE28 starting...");

  watch = TTGOClass::getWatch();
  watch->begin();
  watch->openBL();

  tft    = watch->tft;
  sensor = watch->bma;

  // Create all four FreeRTOS semaphore handles
  tftMutex          = xSemaphoreCreateMutex();
  sharedMutex       = xSemaphoreCreateMutex();
  rtcStateMutex     = xSemaphoreCreateMutex();
  displayStateMutex = xSemaphoreCreateMutex();

  // Initial screen
  if (xSemaphoreTake(tftMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    tft->fillScreen(TFT_BLACK);
    drawStatusBar();
    xSemaphoreGive(tftMutex);
  }

  // Flash init (scans headers, restores state, pre-erases unused sectors)
  flashInit();

  // Default threshold from loaded config or fallback
  if (current_ACCL_NOISE_THRESHOLD < 1.0f) {
    current_ACCL_NOISE_THRESHOLD = (currentMode == MODE_ANXIETY)
      ? DEFAULT_ANXIETY_THRESHOLD
      : DEFAULT_TREMOR_THRESHOLD;
  }

  // PEK (side button) short press IRQ
  pinMode(AXP202_INT, INPUT_PULLUP);
  attachInterrupt(AXP202_INT, [] { pmuIRQ = true; }, FALLING);
  if (watch->power) {
    watch->power->enableIRQ(AXP202_PEK_SHORTPRESS_IRQ, true);
    watch->power->clearIRQ();
  }

  // Battery info
  if (watch->power) {
    int pct = watch->power->getBattPercentage();
    Serial.printf("Battery: %d%%  %s\n", pct,
                  watch->power->isChargeing() ? "(Charging)" : "");
  }

  // BMA423 accelerometer config
  Acfg cfg;
  cfg.odr       = BMA4_OUTPUT_DATA_RATE_100HZ;
  cfg.range     = BMA4_ACCEL_RANGE_2G;
  cfg.perf_mode = BMA4_CONTINUOUS_MODE;
  sensor->accelConfig(cfg);
  sensor->enableAccel();

  // Start flash recording for TREMOR mode (default)
  if (currentMode == MODE_TREMOR) {
    flashStartRecording();
  }

  // T1 sync timer starts at boot
  t1_start_ms = millis();

  // If sync was interrupted before reboot, mark for resume on BLE connect
  if (sync_interrupted && syncState == SYNC_INTERRUPTED) {
    sync_pending = true;
  }

  // BLE init — device name "iGest v3"
  BLEDevice::init("iGest v3");

  pServer = BLEDevice::createServer();
  pServer->setCallbacks(new MyServerCallbacks());

  // IMU Service (notify — 238-byte anxiety B2 packets)
  imuService = pServer->createService(IMU_SERVICE_UUID);
  imuCharacteristic = imuService->createCharacteristic(
    IMU_CHARACTERISTIC_UUID,
    BLECharacteristic::PROPERTY_READ  |
    BLECharacteristic::PROPERTY_WRITE |
    BLECharacteristic::PROPERTY_NOTIFY
  );
  imuCharacteristic->addDescriptor(new BLE2902());
  imuService->start();

  // TIME Service (write — accepts time/mode/sens)
  timeService = pServer->createService(TIME_SERVICE_UUID);
  timeCharacteristic = timeService->createCharacteristic(
    TIME_CHARACTERISTIC_UUID,
    BLECharacteristic::PROPERTY_WRITE
  );
  timeCharacteristic->setCallbacks(new TimeCharacteristicCallbacks());
  timeService->start();

  // FLASH Service (notify data + write cmd)
  flashService = pServer->createService(FLASH_SERVICE_UUID);
  flashDataCharacteristic = flashService->createCharacteristic(
    FLASH_DATA_CHAR_UUID,
    BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY
  );
  flashDataCharacteristic->addDescriptor(new BLE2902());
  flashCmdCharacteristic = flashService->createCharacteristic(
    FLASH_CMD_CHAR_UUID,
    BLECharacteristic::PROPERTY_WRITE
  );
  flashCmdCharacteristic->setCallbacks(new FlashCmdCharacteristicCallbacks());
  flashService->start();

  // Advertise all three services
  BLEAdvertising* pAdvertising = BLEDevice::getAdvertising();
  pAdvertising->addServiceUUID(IMU_SERVICE_UUID);
  pAdvertising->addServiceUUID(TIME_SERVICE_UUID);
  pAdvertising->addServiceUUID(FLASH_SERVICE_UUID);
  pAdvertising->start();

  Serial.println("BLE advertising as 'iGest v3'...");

  // Display task on Core 1, priority 1
  xTaskCreatePinnedToCore(displayTask, "displayTask", 4096, NULL, 1, NULL, 1);

  pinMode(TOUCH_INT, INPUT);

  uint32_t now = millis();
  lastDisplayOnMs      = now;
  displayOn            = true;
  displayWakeRequested = true;
  forceRedraw          = true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Section 19: loop() — three non-blocking streams
// ─────────────────────────────────────────────────────────────────────────────

void loop() {

  // ══════════════════════════════════════════════════════════════════════════
  // STREAM 1: Input & Sampling
  // ══════════════════════════════════════════════════════════════════════════

  uint32_t nowTap = millis();

  // Touch tap handling (non-blocking double-tap wake)
  if (detectTap(nowTap)) handleTap(nowTap);
  if (waitingSecondTap && (nowTap - firstTapMs > DOUBLE_TAP_GAP_MS)) {
    waitingSecondTap = false;
  }

  // PEK (side button) short press wake
  if (pmuIRQ) {
    pmuIRQ = false;
    if (watch->power) {
      watch->power->readIRQ();
      if (watch->power->isPEKShortPressIRQ()) {
        watch->power->clearIRQ();
        requestDisplayWake(millis());
      } else {
        watch->power->clearIRQ();
      }
    }
  }

  // Auto-off after DISPLAY_ON_WINDOW_MS of inactivity
  {
    uint32_t lastOnLocal = 0;
    bool     dispOnLocal = false;
    if (displayStateMutex && xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
      dispOnLocal  = displayOn;
      lastOnLocal  = lastDisplayOnMs;
      xSemaphoreGive(displayStateMutex);
    } else {
      dispOnLocal = displayOn;
      lastOnLocal = lastDisplayOnMs;
    }
    if (dispOnLocal && (nowTap - lastOnLocal >= DISPLAY_ON_WINDOW_MS)) {
      if (displayStateMutex &&
          xSemaphoreTake(displayStateMutex, 0) == pdTRUE) {
        displayOn = false;
        xSemaphoreGive(displayStateMutex);
      } else {
        displayOn = false;
      }
    }
  }

  // RTC connection monitor
  monitorConnectionStateForRTC(wasConnected);

  // Parse incoming BLE writes to timeCharacteristic
  if (timeCharWritten) {
    String receivedTime = String(receivedTimeBuffer);
    receivedTime.trim();
    Serial.print("BLE: Received '"); Serial.print(receivedTime); Serial.println("'");

    // ── Parse MODE:<value> ────────────────────────────────────────────────
    String upper = receivedTime;
    upper.toUpperCase();
    int idx = upper.indexOf("MODE:");
    if (idx >= 0) {
      int valStart = idx + 5;
      while (valStart < (int)receivedTime.length() &&
             isSpace(receivedTime[valStart])) valStart++;
      int valEnd = valStart;
      while (valEnd < (int)receivedTime.length() &&
             receivedTime[valEnd] != ' ' &&
             receivedTime[valEnd] != '\n' &&
             receivedTime[valEnd] != '\r') valEnd++;

      String modeValue = (valEnd > valStart)
                         ? receivedTime.substring(valStart, valEnd)
                         : String("");
      modeValue.trim();
      String m = modeValue; m.toUpperCase();

      if (m.length() > 0) {
        uint8_t newMode = currentMode;
        if (m.indexOf("ANX") >= 0) {
          newMode = MODE_ANXIETY;
          current_ACCL_NOISE_THRESHOLD = DEFAULT_ANXIETY_THRESHOLD;
        } else if (m.indexOf("TREM") >= 0) {
          newMode = MODE_TREMOR;
          current_ACCL_NOISE_THRESHOLD = DEFAULT_TREMOR_THRESHOLD;
        }

        if (newMode != currentMode) {
          currentMode = newMode;
          if (xSemaphoreTake(sharedMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
            tremor_count = 0;
            xSemaphoreGive(sharedMutex);
          } else {
            tremor_count = 0;
          }
          flashSaveConfig();
          forceRedraw = true;
          Serial.printf("MODE: Switched to %s  threshold=%.0f\n",
                        getModeString(), current_ACCL_NOISE_THRESHOLD);

          // If we just switched to ANXIETY while BLE is already connected and
          // there is unsynced flash data, start a sync session immediately.
          // (Mirrors the onConnect() check that was missed on first connection.)
          if (currentMode == MODE_ANXIETY && wasConnected &&
              sync_read_offset < flash_write_offset) {
            sync_mark_offset = flash_write_offset;
            sync_pending     = true;
            syncState        = SYNC_STARTING;
            flashSaveConfig();
            Serial.printf("SYNC: Mode-switch anxiety on-connect — syncing %lu bytes\n",
                          sync_mark_offset - sync_read_offset);
          }
        }
      }

      int removeEnd = (valEnd > valStart) ? valEnd : (idx + 5);
      String tokenToRemove = receivedTime.substring(idx, removeEnd);
      receivedTime.replace(tokenToRemove, "");
      receivedTime.trim();
    }

    // ── Parse SENS:<value> (anxiety only) ────────────────────────────────
    String upper2 = receivedTime;
    upper2.toUpperCase();
    int sidx = upper2.indexOf("SENS:");
    if (sidx >= 0) {
      int valStart = sidx + 5;
      while (valStart < (int)receivedTime.length() &&
             isSpace(receivedTime[valStart])) valStart++;
      int valEnd = valStart;
      while (valEnd < (int)receivedTime.length() &&
             receivedTime[valEnd] != ' ' &&
             receivedTime[valEnd] != '\n' &&
             receivedTime[valEnd] != '\r') valEnd++;

      String sensStr = (valEnd > valStart)
                       ? receivedTime.substring(valStart, valEnd)
                       : String("");
      sensStr.trim();

      if (currentMode == MODE_ANXIETY && sensStr.length() > 0) {
        int sensInt = sensStr.toInt();
        if (sensInt < 50)  sensInt = 50;
        if (sensInt > 500) sensInt = 500;
        current_ACCL_NOISE_THRESHOLD = (float)sensInt;
        flashSaveConfig();
        Serial.printf("SENS: ANXIETY threshold = %.0f\n",
                      current_ACCL_NOISE_THRESHOLD);
      }

      int removeEnd = (valEnd > valStart) ? valEnd : (sidx + 5);
      String tokenToRemove = receivedTime.substring(sidx, removeEnd);
      receivedTime.replace(tokenToRemove, "");
      receivedTime.trim();
    }

    // ── Parse HH:mm|ddMMyyyy ─────────────────────────────────────────────
    if (receivedTime.length() > 0) {
      String timePart       = receivedTime;
      String datePartDigits = "";
      int    bar = receivedTime.indexOf('|');
      if (bar >= 0) {
        timePart       = receivedTime.substring(0, bar); timePart.trim();
        datePartDigits = receivedTime.substring(bar + 1); datePartDigits.trim();
      } else {
        timePart.trim();
      }

      int hh = 0, mm = 0, ss = 0;
      int c1 = timePart.indexOf(':');
      int c2 = (c1 >= 0) ? timePart.indexOf(':', c1 + 1) : -1;
      if (c1 >= 0 && c2 >= 0) {
        hh = timePart.substring(0, c1).toInt();
        mm = timePart.substring(c1+1, c2).toInt();
        ss = timePart.substring(c2+1).toInt();
      } else if (c1 >= 0) {
        hh = timePart.substring(0, c1).toInt();
        mm = timePart.substring(c1+1).toInt();
      }

      int dd = 0, mon = 0, yyyy = 0;
      String dateFormatted = "";
      if (datePartDigits.length() >= 8) {
        String ddStr   = datePartDigits.substring(0, 2);
        String mmStr   = datePartDigits.substring(2, 4);
        String yyyyStr = datePartDigits.substring(4, 8);
        dd   = ddStr.toInt();
        mon  = mmStr.toInt();
        yyyy = yyyyStr.toInt();
        dateFormatted = ddStr + "-" + mmStr + "-" + yyyyStr;
      }

      if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59 &&
          ss >= 0 && ss <= 59 && dd >= 1 && dd <= 31 &&
          mon >= 1 && mon <= 12 && yyyy >= 2000) {
        updateRTCReference(hh, mm, ss, dd, mon, yyyy);

        if (xSemaphoreTake(sharedMutex, pdMS_TO_TICKS(20)) == pdTRUE) {
          char hmbuf[6];
          snprintf(hmbuf, sizeof(hmbuf), "%02d:%02d", hh, mm);
          lastDisplayedTime = String(hmbuf);
          if (dateFormatted.length() > 0) lastDisplayedDate = dateFormatted;
          xSemaphoreGive(sharedMutex);
        } else {
          char hmbuf[6];
          snprintf(hmbuf, sizeof(hmbuf), "%02d:%02d", hh, mm);
          lastDisplayedTime = String(hmbuf);
          if (dateFormatted.length() > 0) lastDisplayedDate = dateFormatted;
        }
        Serial.printf("BLE: Time=%s  Date=%s\n",
                      timePart.c_str(), dateFormatted.c_str());
      }
    }

    timeCharWritten = false;
  }

  // ── 10ms Sampling Wait Throttle (non-blocking) ────────────────────────────
  bool doSample = (millis() - lastReadTime >= SAMPLE_INTERVAL_MS);

  if (doSample) {
    // Read BMA423 accelerometer
    Accel acc;
    if (sensor->getAccel(acc)) {
      IMU_data[1] = acc.x;
      IMU_data[2] = acc.y;
      IMU_data[3] = acc.z;
    }

    lastReadTime = millis();
    currentTime  = lastReadTime;

    // IIR filter each axis
    for (int i = 0; i < IMU_AXIS; i++) {
      IMU_filtered[i] = applyIIRFilter(IMU_data[i + 1], i);
    }
    calculateMagnitudes();

    // Update circular buffer (mutex-protected for display safety)
    if (xSemaphoreTake(sharedMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
      realw_timestamps[dataIndex] = currentTime;
      Accel_Mag[dataIndex]        = IMU_magnitude[0];
      dataIndex = (dataIndex + 1) % MAX_DATA_POINTS;
      if (dataIndex == 0) bufferFilled = true;
      xSemaphoreGive(sharedMutex);
    } else {
      realw_timestamps[dataIndex] = currentTime;
      Accel_Mag[dataIndex]        = IMU_magnitude[0];
      dataIndex = (dataIndex + 1) % MAX_DATA_POINTS;
      if (dataIndex == 0) bufferFilled = true;
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // STREAM 2: Mode-Aware Output (runs once per 10ms sample, after buffer fills)
  // ══════════════════════════════════════════════════════════════════════════

  if (doSample && bufferFilled) {
    // Tremor detection — runs ALWAYS regardless of mode or connection
    float Amag_Baseline = calculateBaseline(Accel_Mag, currentTime);
    crossingCount_Amag  = countCrossings(Accel_Mag, Amag_Baseline,
                                         currentTime,
                                         current_ACCL_NOISE_THRESHOLD);
    Amag_tremor = (crossingCount_Amag >= CROSSING_THRESHOLD);

    bool current_tremor_state = Amag_tremor;

    // Tremor Onset
    if (current_tremor_state && !prev_tremor_state && !inTremorWindow) {
      // Flush any partial buffer on the active path
      if (!(currentMode == MODE_ANXIETY && wasConnected)) {
        flashFlushPartialB1();
      }

      if (xSemaphoreTake(sharedMutex, pdMS_TO_TICKS(5)) == pdTRUE) {
        tremor_count++;
        xSemaphoreGive(sharedMutex);
      } else {
        tremor_count++;
      }

      // Write START marker: live anxiety path notifies via IMU UUID;
      // all other paths write to flash via B1.
      if (currentMode == MODE_ANXIETY && wasConnected) {
        imuSendMarker(MARKER_TREMOR_START_H1, MARKER_TREMOR_START_H2, NULL, 0);
      } else {
        b1AddMarker(MARKER_TREMOR_START_H1, MARKER_TREMOR_START_H2, NULL, 0);
      }

      inTremorWindow      = true;
      tremorWindowStartMs = millis();
      bleStartTime        = tremorWindowStartMs;
      tremor_data_count   = 1;
    }
    prev_tremor_state = current_tremor_state;

    // Shared time difference and sample status
    time_difference = millis() - bleStartTime;
    uint8_t sample_status = (uint8_t)(inTremorWindow ? 1 :
                                      (Amag_tremor  ? 1 : 0));

    // ── ROUTING ───────────────────────────────────────────────────────────

    if (currentMode == MODE_ANXIETY && wasConnected) {
      // ── ANXIETY + BLE connected: fill B2, send via imuCharacteristic
      //    DO NOT WRITE TO FLASH
      b2AddRecord(IMU_magnitude[0], (uint16_t)time_difference, sample_status);
      tremor_data_count++;

      if (b2_fill >= B2_SAMPLES) {
        b2SendPacket();
      }

      // Tremor Expiration (anxiety live path — end + zero markers via IMU UUID)
      if (inTremorWindow &&
          (millis() - tremorWindowStartMs > BLE_DURATION)) {
        uint8_t endPayload[4];
        uint16_t tc  = (uint16_t)tremor_count;
        uint16_t tdc = (uint16_t)tremor_data_count;
        memcpy(&endPayload[0], &tc,  2);
        memcpy(&endPayload[2], &tdc, 2);
        imuSendMarker(MARKER_TREMOR_END_H1, MARKER_TREMOR_END_H2,
                      endPayload, 4);
        imuSendMarker(MARKER_ZERO_H1, MARKER_ZERO_H2, NULL, 0);
        inTremorWindow    = false;
        tremor_data_count = 1;
      }

    } else {
      // ── TREMOR (any) OR ANXIETY offline: fill B1, flush to flash
      if (!flash_erase_in_progress) {
        uint16_t rtcNow = flashGetRTCTime();
        b1AddRecord(IMU_magnitude[0], (uint16_t)time_difference,
                    rtcNow, sample_status);
        tremor_data_count++;

        // Tremor Expiration (flash path)
        if (inTremorWindow &&
            (millis() - tremorWindowStartMs > BLE_DURATION)) {
          flashFlushPartialB1();

          uint8_t endPayload[4];
          uint16_t tc  = (uint16_t)tremor_count;
          uint16_t tdc = (uint16_t)tremor_data_count;
          memcpy(&endPayload[0], &tc,  2);
          memcpy(&endPayload[2], &tdc, 2);
          b1AddMarker(MARKER_TREMOR_END_H1, MARKER_TREMOR_END_H2,
                      endPayload, 4);
          b1AddMarker(MARKER_ZERO_H1, MARKER_ZERO_H2, NULL, 0);

          tremor_data_count = 1;
          inTremorWindow    = false;
        }
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // STREAM 3: Sync & Flash Tasks (unconditional)
  // ══════════════════════════════════════════════════════════════════════════

  // REFRESH command: immediate sync trigger
  if (flashRefreshRequested) {
    flashRefreshRequested = false;
    sync_mark_offset = flash_write_offset;
    sync_pending     = true;
    syncState        = wasConnected ? SYNC_STARTING : SYNC_WAITING_BLE;
    flashSaveConfig();
    Serial.println("SYNC: REFRESH command — immediate trigger");
  }

  // Non-blocking sync state machine
  syncStep();
}
