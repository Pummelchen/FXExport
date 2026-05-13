#property strict
#property version   "1.000"
#property description "FXExport localhost history bridge. Swift owns validation, storage, checkpoints, verification, and repair."

input string SwiftHost = "127.0.0.1";
input ushort SwiftPort = 5055;
input uint ConnectTimeoutMs = 3000;
input uint ReadTimeoutMs = 100;
input int TimerSeconds = 1;
input int MaxBarsPerResponse = 50000;

int g_socket = INVALID_HANDLE;
bool g_connected = false;

int OnInit()
{
   EventSetTimer(TimerSeconds);
   Print("FXExport EA initialized. Waiting to connect to Swift at ", SwiftHost, ":", SwiftPort);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   CloseSocket();
}

void OnTimer()
{
   if(!EnsureConnected())
      return;

   while(SocketIsConnected(g_socket) && SocketIsReadable(g_socket) >= 4)
   {
      if(!ProcessOneFrame())
         break;
   }
}

bool EnsureConnected()
{
   if(g_socket != INVALID_HANDLE && SocketIsConnected(g_socket))
      return true;

   CloseSocket();
   ResetLastError();
   g_socket = SocketCreate(SOCKET_DEFAULT);
   if(g_socket == INVALID_HANDLE)
   {
      Print("MT5Research bridge SocketCreate failed. error=", GetLastError());
      return false;
   }

   ResetLastError();
   if(!SocketConnect(g_socket, SwiftHost, SwiftPort, ConnectTimeoutMs))
   {
      Print("MT5Research bridge connect failed. error=", GetLastError(), " target=", SwiftHost, ":", SwiftPort);
      CloseSocket();
      return false;
   }

   Print("MT5Research bridge connected to Swift at ", SwiftHost, ":", SwiftPort);
   g_connected = true;
   return true;
}

void CloseSocket()
{
   if(g_socket != INVALID_HANDLE)
   {
      SocketClose(g_socket);
      g_socket = INVALID_HANDLE;
   }
   g_connected = false;
}

bool ProcessOneFrame()
{
   uchar header[];
   ArrayResize(header, 4);
   if(!ReadExact(header, 4))
   {
      Print("MT5Research bridge failed to read frame header. error=", GetLastError());
      CloseSocket();
      return false;
   }

   int bodyLength = ((int)header[0] << 24) | ((int)header[1] << 16) | ((int)header[2] << 8) | (int)header[3];
   if(bodyLength <= 0 || bodyLength > 16777216)
   {
      Print("MT5Research bridge invalid frame length: ", bodyLength);
      CloseSocket();
      return false;
   }

   uchar body[];
   ArrayResize(body, bodyLength);
   if(!ReadExact(body, bodyLength))
   {
      Print("MT5Research bridge partial frame read. expected=", bodyLength, " error=", GetLastError());
      CloseSocket();
      return false;
   }

   string json = CharArrayToString(body, 0, bodyLength, CP_UTF8);
   HandleRequest(json);
   return true;
}

void HandleRequest(const string json)
{
   string requestId = JsonStringField(json, "request_id");
   string command = JsonStringField(json, "command");
   string payload = ExtractPayload(json);
   string expectedChecksum = JsonStringField(json, "payload_checksum");
   int expectedLength = (int)JsonLongField(json, "payload_length");
   long schemaVersion = JsonLongField(json, "schema_version");

   if(requestId == "" || command == "" || payload == "")
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "Missing request_id, command, or payload");
      return;
   }

   if(schemaVersion != 1)
   {
      SendError(requestId, command, "UNSUPPORTED_SCHEMA_VERSION", "Expected schema_version 1");
      return;
   }

   if(PayloadByteLength(payload) != expectedLength)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "Payload length mismatch");
      return;
   }

   string actualChecksum = PayloadChecksum(payload);
   if(actualChecksum != expectedChecksum)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "Payload checksum mismatch");
      return;
   }

   if(command == "HELLO")
      SendOK(requestId, command, "{\"bridge_name\":\"FXExport\",\"bridge_version\":\"0.1\",\"schema_version\":1}");
   else if(command == "PING")
      SendOK(requestId, command, "{}");
   else if(command == "GET_TERMINAL_INFO")
      HandleTerminalInfo(requestId, command);
   else if(command == "GET_SERVER_TIME_SNAPSHOT")
      HandleServerTimeSnapshot(requestId, command);
   else if(command == "PREPARE_SYMBOL")
      HandlePrepareSymbol(requestId, command, payload);
   else if(command == "GET_SYMBOL_INFO")
      HandleSymbolInfo(requestId, command, payload);
   else if(command == "GET_HISTORY_STATUS")
      HandleHistoryStatus(requestId, command, payload);
   else if(command == "ENSURE_M1_MONTH_HISTORY")
      HandleEnsureM1MonthHistory(requestId, command, payload);
   else if(command == "GET_OLDEST_M1_BAR_TIME")
      HandleOldestM1(requestId, command, payload);
   else if(command == "GET_LATEST_CLOSED_M1_BAR")
      HandleLatestClosedM1(requestId, command, payload);
   else if(command == "GET_RATES_RANGE")
      HandleRatesRange(requestId, command, payload);
   else if(command == "GET_RATES_FROM_POSITION")
      HandleRatesFromPosition(requestId, command, payload);
   else
      SendError(requestId, command, "UNKNOWN_COMMAND", "Unsupported command");
}

void HandleTerminalInfo(const string requestId, const string command)
{
   string payload = "{";
   payload += "\"terminal_name\":\"" + JsonEscape(TerminalInfoString(TERMINAL_NAME)) + "\",";
   payload += "\"company\":\"" + JsonEscape(AccountInfoString(ACCOUNT_COMPANY)) + "\",";
   payload += "\"server\":\"" + JsonEscape(AccountInfoString(ACCOUNT_SERVER)) + "\",";
   payload += "\"account_login\":" + IntegerToString((long)AccountInfoInteger(ACCOUNT_LOGIN)) + ",";
   payload += "\"account_currency\":\"" + JsonEscape(AccountInfoString(ACCOUNT_CURRENCY)) + "\",";
   payload += "\"account_leverage\":" + IntegerToString((long)AccountInfoInteger(ACCOUNT_LEVERAGE)) + ",";
   payload += "\"account_margin_mode\":" + IntegerToString((long)AccountInfoInteger(ACCOUNT_MARGIN_MODE));
   payload += "}";
   SendOK(requestId, command, payload);
}

void HandleServerTimeSnapshot(const string requestId, const string command)
{
   string payload = "{";
   payload += "\"time_trade_server\":" + IntegerToString((long)TimeTradeServer()) + ",";
   payload += "\"time_gmt\":" + IntegerToString((long)TimeGMT()) + ",";
   payload += "\"time_local\":" + IntegerToString((long)TimeLocal());
   payload += "}";
   SendOK(requestId, command, payload);
}

void HandlePrepareSymbol(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   ResetLastError();
   bool selected = SymbolSelect(symbol, true);
   if(!selected)
   {
      SendError(requestId, command, "SYMBOL_NOT_FOUND", "SymbolSelect failed for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }
   SendSymbolInfo(requestId, command, symbol, selected);
}

void HandleSymbolInfo(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   bool selected = SymbolInfoInteger(symbol, SYMBOL_SELECT);
   if(!selected)
   {
      SendError(requestId, command, "SYMBOL_NOT_SELECTED", "Symbol is not selected in Market Watch: " + symbol);
      return;
   }
   SendSymbolInfo(requestId, command, symbol, selected);
}

void SendSymbolInfo(const string requestId, const string command, const string symbol, const bool selected)
{
   long digits = SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   long spread = SymbolInfoInteger(symbol, SYMBOL_SPREAD);
   long spreadFloat = SymbolInfoInteger(symbol, SYMBOL_SPREAD_FLOAT);
   long swapMode = SymbolInfoInteger(symbol, SYMBOL_SWAP_MODE);
   long tradeCalcMode = SymbolInfoInteger(symbol, SYMBOL_TRADE_CALC_MODE);
   long tradeMode = SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   double contractSize = SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
   double volumeMin = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double volumeStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   double volumeMax = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double swapLong = SymbolInfoDouble(symbol, SYMBOL_SWAP_LONG);
   double swapShort = SymbolInfoDouble(symbol, SYMBOL_SWAP_SHORT);
   double marginInitial = SymbolInfoDouble(symbol, SYMBOL_MARGIN_INITIAL);
   double marginMaintenance = SymbolInfoDouble(symbol, SYMBOL_MARGIN_MAINTENANCE);
   double tickSize = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
   double tickValue = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickValueProfit = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE_PROFIT);
   double tickValueLoss = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE_LOSS);
   double marginCalcLots = MarginCalcLots(volumeMin, volumeMax);
   double marginBuy = 0.0;
   double marginSell = 0.0;
   bool hasBuyMargin = OrderCalcMargin(ORDER_TYPE_BUY, symbol, marginCalcLots, ask, marginBuy);
   bool hasSellMargin = OrderCalcMargin(ORDER_TYPE_SELL, symbol, marginCalcLots, bid, marginSell);

   string payload = "{";
   payload += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   payload += "\"selected\":" + (selected ? "true" : "false") + ",";
   payload += "\"digits\":" + IntegerToString(digits) + ",";
   payload += "\"bid\":" + JsonDouble(bid, (int)digits) + ",";
   payload += "\"ask\":" + JsonDouble(ask, (int)digits) + ",";
   payload += "\"point\":" + JsonDouble(point, 12) + ",";
   payload += "\"spread\":" + IntegerToString(spread) + ",";
   payload += "\"spread_float\":" + (spreadFloat != 0 ? "true" : "false") + ",";
   payload += "\"contract_size\":" + JsonDouble(contractSize, 8) + ",";
   payload += "\"volume_min\":" + JsonDouble(volumeMin, 8) + ",";
   payload += "\"volume_step\":" + JsonDouble(volumeStep, 8) + ",";
   payload += "\"volume_max\":" + JsonDouble(volumeMax, 8) + ",";
   payload += "\"swap_long\":" + JsonDouble(swapLong, 8) + ",";
   payload += "\"swap_short\":" + JsonDouble(swapShort, 8) + ",";
   payload += "\"swap_mode\":" + IntegerToString(swapMode) + ",";
   payload += "\"margin_initial\":" + JsonOptionalDouble(marginInitial, 8) + ",";
   payload += "\"margin_maintenance\":" + JsonOptionalDouble(marginMaintenance, 8) + ",";
   payload += "\"margin_buy\":" + (hasBuyMargin ? JsonDouble(marginBuy, 8) : "null") + ",";
   payload += "\"margin_sell\":" + (hasSellMargin ? JsonDouble(marginSell, 8) : "null") + ",";
   payload += "\"margin_calc_lots\":" + JsonDouble(marginCalcLots, 8) + ",";
   payload += "\"trade_calc_mode\":" + IntegerToString(tradeCalcMode) + ",";
   payload += "\"trade_mode\":" + IntegerToString(tradeMode) + ",";
   payload += "\"tick_size\":" + JsonDouble(tickSize, 12) + ",";
   payload += "\"tick_value\":" + JsonDouble(tickValue, 8) + ",";
   payload += "\"tick_value_profit\":" + JsonOptionalDouble(tickValueProfit, 8) + ",";
   payload += "\"tick_value_loss\":" + JsonOptionalDouble(tickValueLoss, 8);
   payload += "}";
   SendOK(requestId, command, payload);
}

double MarginCalcLots(const double volumeMin, const double volumeMax)
{
   double lots = 1.0;
   if(volumeMin > 0.0 && lots < volumeMin)
      lots = volumeMin;
   if(volumeMax > 0.0 && lots > volumeMax)
      lots = volumeMax;
   return lots;
}

void HandleHistoryStatus(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   long synchronized = 0;
   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SYNCHRONIZED, synchronized);
   int bars = Bars(symbol, PERIOD_M1);
   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"synchronized\":" + (synchronized != 0 ? "true" : "false") + ",";
   response += "\"bars\":" + IntegerToString(bars);
   response += "}";
   SendOK(requestId, command, response);
}

void HandleEnsureM1MonthHistory(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   long monthStart = JsonLongField(payload, "month_start_mt5_server_ts");
   long monthEndExclusive = JsonLongField(payload, "month_end_mt5_server_ts_exclusive");

   if(symbol == "")
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "ENSURE_M1_MONTH_HISTORY missing mt5_symbol");
      return;
   }
   if(monthStart < 0 || monthEndExclusive <= monthStart || (monthStart % 60) != 0 || (monthEndExclusive % 60) != 0)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "ENSURE_M1_MONTH_HISTORY requires a non-negative minute-aligned month range");
      return;
   }

   bool selected = SymbolInfoInteger(symbol, SYMBOL_SELECT);
   if(!selected)
   {
      ResetLastError();
      selected = SymbolSelect(symbol, true);
   }
   if(!selected)
   {
      SendError(requestId, command, "SYMBOL_NOT_FOUND", "SymbolSelect failed for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }

   MqlRates latest[];
   ArraySetAsSeries(latest, true);
   ResetLastError();
   int latestCopied = CopyRates(symbol, PERIOD_M1, 1, 1, latest);
   if(latestCopied != 1)
   {
      SendError(requestId, command, "NO_CLOSED_BAR", "Could not determine latest closed M1 bar for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }

   long latestClosed = (long)latest[0].time;
   long effectiveToExclusive = monthEndExclusive;
   if(effectiveToExclusive > latestClosed + 60)
      effectiveToExclusive = latestClosed + 60;

   long synchronizedBefore = 0;
   long synchronizedAfter = 0;
   long serverFirstDate = 0;
   long localFirstBefore = 0;
   long localFirstAfter = 0;

   ResetLastError();
   bool hasServerFirstDate = SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SERVER_FIRSTDATE, serverFirstDate);
   int serverFirstError = GetLastError();
   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SYNCHRONIZED, synchronizedBefore);
   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_FIRSTDATE, localFirstBefore);

   int totalBarsBefore = Bars(symbol, PERIOD_M1);
   int rangeBarsBefore = 0;
   int rangeBarsAfter = 0;
   int copied = 0;
   int lastError = 0;
   long firstCopied = 0;
   long lastCopied = 0;
   bool loadAttempted = false;

   if(effectiveToExclusive <= monthStart)
   {
      SendMonthHistoryStatus(
         requestId, command, symbol, monthStart, monthEndExclusive, effectiveToExclusive,
         serverFirstDate, localFirstBefore, localFirstBefore,
         0, 0, totalBarsBefore, totalBarsBefore,
         synchronizedBefore != 0, synchronizedBefore != 0,
         false, false, false, true,
         0, 0, 0, 0, "future"
      );
      return;
   }

   bool historicalAvailable = hasServerFirstDate && serverFirstDate > 0 && effectiveToExclusive > serverFirstDate;
   if(!historicalAvailable)
   {
      SendMonthHistoryStatus(
         requestId, command, symbol, monthStart, monthEndExclusive, effectiveToExclusive,
         serverFirstDate, localFirstBefore, localFirstBefore,
         0, 0, totalBarsBefore, totalBarsBefore,
         synchronizedBefore != 0, synchronizedBefore != 0,
         false, false, false, true,
         0, 0, 0, serverFirstError, "unavailable"
      );
      return;
   }

   ResetLastError();
   rangeBarsBefore = Bars(symbol, PERIOD_M1, (datetime)monthStart, (datetime)(effectiveToExclusive - 60));
   int barsBeforeError = GetLastError();
   bool alreadyLoaded = (rangeBarsBefore > 0 && synchronizedBefore != 0);

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   loadAttempted = true;
   ResetLastError();
   copied = CopyRates(symbol, PERIOD_M1, (datetime)monthStart, (datetime)(effectiveToExclusive - 60), rates);
   lastError = GetLastError();
   if(copied < 0)
   {
      copied = 0;
      if(lastError == 0)
         lastError = barsBeforeError;
   }

   if(copied > 0)
   {
      firstCopied = (long)rates[0].time;
      lastCopied = (long)rates[copied - 1].time;
   }

   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SYNCHRONIZED, synchronizedAfter);
   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_FIRSTDATE, localFirstAfter);
   int totalBarsAfter = Bars(symbol, PERIOD_M1);
   ResetLastError();
   rangeBarsAfter = Bars(symbol, PERIOD_M1, (datetime)monthStart, (datetime)(effectiveToExclusive - 60));
   int barsAfterError = GetLastError();
   if(lastError == 0)
      lastError = barsAfterError;

   bool loadComplete = (rangeBarsAfter > 0 && synchronizedAfter != 0);
   string status = "loading";
   if(loadComplete)
      status = (copied > 0 || alreadyLoaded ? "loaded" : "partial");
   else if(copied > 0)
      status = "partial";

   SendMonthHistoryStatus(
      requestId, command, symbol, monthStart, monthEndExclusive, effectiveToExclusive,
      serverFirstDate, localFirstBefore, localFirstAfter,
      rangeBarsBefore, rangeBarsAfter, totalBarsBefore, totalBarsAfter,
      synchronizedBefore != 0, synchronizedAfter != 0,
      historicalAvailable, alreadyLoaded, loadAttempted, loadComplete,
      copied, firstCopied, lastCopied, lastError, status
   );
}

void SendMonthHistoryStatus(
   const string requestId,
   const string command,
   const string symbol,
   const long monthStart,
   const long monthEndExclusive,
   const long effectiveToExclusive,
   const long serverFirstDate,
   const long localFirstBefore,
   const long localFirstAfter,
   const int rangeBarsBefore,
   const int rangeBarsAfter,
   const int totalBarsBefore,
   const int totalBarsAfter,
   const bool synchronizedBefore,
   const bool synchronizedAfter,
   const bool historicalAvailable,
   const bool alreadyLoaded,
   const bool loadAttempted,
   const bool loadComplete,
   const int copied,
   const long firstCopied,
   const long lastCopied,
   const int lastError,
   const string status
)
{
   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"timeframe\":\"M1\",";
   response += "\"month_start_mt5_server_ts\":" + IntegerToString(monthStart) + ",";
   response += "\"month_end_mt5_server_ts_exclusive\":" + IntegerToString(monthEndExclusive) + ",";
   response += "\"effective_to_mt5_server_ts_exclusive\":" + IntegerToString(effectiveToExclusive) + ",";
   response += "\"server_first_date_mt5_server_ts\":" + IntegerToString(serverFirstDate) + ",";
   response += "\"local_first_date_before_mt5_server_ts\":" + IntegerToString(localFirstBefore) + ",";
   response += "\"local_first_date_after_mt5_server_ts\":" + IntegerToString(localFirstAfter) + ",";
   response += "\"range_bars_before\":" + IntegerToString(rangeBarsBefore) + ",";
   response += "\"range_bars_after\":" + IntegerToString(rangeBarsAfter) + ",";
   response += "\"total_bars_before\":" + IntegerToString(totalBarsBefore) + ",";
   response += "\"total_bars_after\":" + IntegerToString(totalBarsAfter) + ",";
   response += "\"series_synchronized_before\":" + (synchronizedBefore ? "true" : "false") + ",";
   response += "\"series_synchronized_after\":" + (synchronizedAfter ? "true" : "false") + ",";
   response += "\"historical_available\":" + (historicalAvailable ? "true" : "false") + ",";
   response += "\"already_loaded\":" + (alreadyLoaded ? "true" : "false") + ",";
   response += "\"load_attempted\":" + (loadAttempted ? "true" : "false") + ",";
   response += "\"load_complete\":" + (loadComplete ? "true" : "false") + ",";
   response += "\"copied_count\":" + IntegerToString(copied) + ",";
   response += "\"first_mt5_server_ts\":" + IntegerToString(firstCopied) + ",";
   response += "\"last_mt5_server_ts\":" + IntegerToString(lastCopied) + ",";
   response += "\"last_error\":" + IntegerToString(lastError) + ",";
   response += "\"status\":\"" + JsonEscape(status) + "\"";
   response += "}";
   SendOK(requestId, command, response);
}

void HandleOldestM1(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   long firstDate = 0;
   ResetLastError();
   if(!SeriesInfoInteger(symbol, PERIOD_M1, SERIES_FIRSTDATE, firstDate) || firstDate <= 0)
   {
      SendError(requestId, command, "NO_HISTORY", "No M1 first date for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }
   SendSingleTime(requestId, command, symbol, firstDate);
}

void HandleLatestClosedM1(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   MqlRates rates[];
   ArraySetAsSeries(rates, true);
   ResetLastError();
   int copied = CopyRates(symbol, PERIOD_M1, 1, 1, rates);
   if(copied != 1)
   {
      SendError(requestId, command, "NO_CLOSED_BAR", "Could not copy latest closed M1 bar for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }
   SendSingleTime(requestId, command, symbol, (long)rates[0].time);
}

void SendSingleTime(const string requestId, const string command, const string symbol, const long mt5ServerTime)
{
   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"mt5_server_time\":" + IntegerToString(mt5ServerTime);
   response += "}";
   SendOK(requestId, command, response);
}

void HandleRatesRange(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   long fromTs = JsonLongField(payload, "from_mt5_server_ts");
   long toExclusive = JsonLongField(payload, "to_mt5_server_ts_exclusive");
   int maxBars = (int)JsonLongField(payload, "max_bars");
   int responseLimit = MaxBarsPerResponse;
   if(responseLimit <= 0)
      responseLimit = 50000;

   if(symbol == "")
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_RANGE missing mt5_symbol");
      return;
   }
   if(fromTs <= 0 || toExclusive <= fromTs)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_RANGE invalid time range");
      return;
   }
   if(maxBars <= 0)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_RANGE max_bars must be positive");
      return;
   }
   if(maxBars > responseLimit)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_RANGE max_bars exceeds MaxBarsPerResponse");
      return;
   }

   MqlRates latest[];
   ArraySetAsSeries(latest, true);
   if(CopyRates(symbol, PERIOD_M1, 1, 1, latest) != 1)
   {
      SendError(requestId, command, "NO_CLOSED_BAR", "Could not determine latest closed M1 bar for " + symbol);
      return;
   }
   long latestClosed = (long)latest[0].time;
   long safeToExclusive = (toExclusive < latestClosed + 60 ? toExclusive : latestClosed + 60);
   long maxToExclusive = fromTs + (long)maxBars * 60;
   if(maxToExclusive > fromTs && maxToExclusive < safeToExclusive)
      safeToExclusive = maxToExclusive;
   long synchronized = 0;
   SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SYNCHRONIZED, synchronized);
   if(safeToExclusive <= fromTs)
   {
      string emptyResponse = "{";
      emptyResponse += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
      emptyResponse += "\"timeframe\":\"M1\",";
      emptyResponse += "\"requested_from_mt5_server_ts\":" + IntegerToString(fromTs) + ",";
      emptyResponse += "\"requested_to_mt5_server_ts_exclusive\":" + IntegerToString(toExclusive) + ",";
      emptyResponse += "\"effective_to_mt5_server_ts_exclusive\":" + IntegerToString(safeToExclusive) + ",";
      emptyResponse += "\"latest_closed_mt5_server_ts\":" + IntegerToString(latestClosed) + ",";
      emptyResponse += "\"series_synchronized\":" + (synchronized != 0 ? "true" : "false") + ",";
      emptyResponse += "\"copied_count\":0,";
      emptyResponse += "\"emitted_count\":0,";
      emptyResponse += "\"first_mt5_server_ts\":0,";
      emptyResponse += "\"last_mt5_server_ts\":0,";
      emptyResponse += "\"rates\":[]";
      emptyResponse += "}";
      SendOK(requestId, command, emptyResponse);
      return;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   ResetLastError();
   int copied = CopyRates(symbol, PERIOD_M1, (datetime)fromTs, (datetime)(safeToExclusive - 60), rates);
   if(copied < 0)
   {
      SendError(requestId, command, "COPY_RATES_FAILED", "CopyRates failed for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   long firstEmitted = 0;
   long lastEmitted = 0;
   string ratesJson = "";
   int emitted = 0;
   for(int i = 0; i < copied && emitted < maxBars; i++)
   {
      long ts = (long)rates[i].time;
      if(ts < fromTs || ts >= safeToExclusive || ts > latestClosed)
         continue;

      if(emitted == 0)
         firstEmitted = ts;
      lastEmitted = ts;
      if(emitted > 0)
         ratesJson += ",";
      ratesJson += "{";
      ratesJson += "\"mt5_server_time\":" + IntegerToString(ts) + ",";
      ratesJson += "\"open\":\"" + DoubleToString(rates[i].open, digits) + "\",";
      ratesJson += "\"high\":\"" + DoubleToString(rates[i].high, digits) + "\",";
      ratesJson += "\"low\":\"" + DoubleToString(rates[i].low, digits) + "\",";
      ratesJson += "\"close\":\"" + DoubleToString(rates[i].close, digits) + "\"";
      ratesJson += "}";
      emitted++;
   }

   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"timeframe\":\"M1\",";
   response += "\"requested_from_mt5_server_ts\":" + IntegerToString(fromTs) + ",";
   response += "\"requested_to_mt5_server_ts_exclusive\":" + IntegerToString(toExclusive) + ",";
   response += "\"effective_to_mt5_server_ts_exclusive\":" + IntegerToString(safeToExclusive) + ",";
   response += "\"latest_closed_mt5_server_ts\":" + IntegerToString(latestClosed) + ",";
   response += "\"series_synchronized\":" + (synchronized != 0 ? "true" : "false") + ",";
   response += "\"copied_count\":" + IntegerToString(copied) + ",";
   response += "\"emitted_count\":" + IntegerToString(emitted) + ",";
   response += "\"first_mt5_server_ts\":" + IntegerToString(firstEmitted) + ",";
   response += "\"last_mt5_server_ts\":" + IntegerToString(lastEmitted) + ",";
   response += "\"rates\":[";
   response += ratesJson;
   response += "]}";
   SendOK(requestId, command, response);
}

void HandleRatesFromPosition(const string requestId, const string command, const string payload)
{
   string symbol = JsonStringField(payload, "mt5_symbol");
   int startPos = (int)JsonLongField(payload, "start_pos");
   int count = (int)JsonLongField(payload, "count");
   int responseLimit = MaxBarsPerResponse;
   if(responseLimit <= 0)
      responseLimit = 50000;

   if(symbol == "")
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_FROM_POSITION missing mt5_symbol");
      return;
   }
   if(startPos < 1)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_FROM_POSITION start_pos must be >= 1 so the open M1 bar is never returned");
      return;
   }
   if(count <= 0)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_FROM_POSITION count must be positive");
      return;
   }
   if(count > responseLimit)
   {
      SendError(requestId, command, "PROTOCOL_ERROR", "GET_RATES_FROM_POSITION count exceeds MaxBarsPerResponse");
      return;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   ResetLastError();
   int copied = CopyRates(symbol, PERIOD_M1, startPos, count, rates);
   if(copied < 0)
   {
      SendError(requestId, command, "COPY_RATES_FAILED", "CopyRates by position failed for " + symbol + ", error=" + IntegerToString(GetLastError()));
      return;
   }

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   SendRatesPayload(requestId, command, symbol, rates, copied, digits);
}

void SendRatesPayload(const string requestId, const string command, const string symbol, MqlRates &rates[], const int copied, const int digits)
{
   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"timeframe\":\"M1\",";
   response += "\"rates\":[";

   bool reverse = false;
   if(copied > 1 && rates[0].time > rates[copied - 1].time)
      reverse = true;

   int emitted = 0;
   for(int j = 0; j < copied; j++)
   {
      int i = reverse ? copied - 1 - j : j;
      long ts = (long)rates[i].time;
      if(emitted > 0)
         response += ",";
      response += "{";
      response += "\"mt5_server_time\":" + IntegerToString(ts) + ",";
      response += "\"open\":\"" + DoubleToString(rates[i].open, digits) + "\",";
      response += "\"high\":\"" + DoubleToString(rates[i].high, digits) + "\",";
      response += "\"low\":\"" + DoubleToString(rates[i].low, digits) + "\",";
      response += "\"close\":\"" + DoubleToString(rates[i].close, digits) + "\"";
      response += "}";
      emitted++;
   }
   response += "]}";
   SendOK(requestId, command, response);
}

void SendOK(const string requestId, const string command, const string payload)
{
   SendEnvelope(requestId, command, payload, "null", "null");
}

void SendError(const string requestId, const string command, const string code, const string message)
{
   SendEnvelope(requestId, command, "{}", "\"" + JsonEscape(code) + "\"", "\"" + JsonEscape(message) + "\"");
}

void SendEnvelope(const string requestId, const string command, const string payload, const string errorCodeJSON, const string errorMessageJSON)
{
   string checksum = PayloadChecksum(payload);
   string envelope = "{";
   envelope += "\"schema_version\":1,";
   envelope += "\"request_id\":\"" + JsonEscape(requestId) + "\",";
   envelope += "\"command\":\"" + JsonEscape(command) + "\",";
   envelope += "\"timestamp_sent_utc\":" + IntegerToString((long)TimeGMT()) + ",";
   envelope += "\"payload_length\":" + IntegerToString(PayloadByteLength(payload)) + ",";
   envelope += "\"payload_checksum\":\"" + checksum + "\",";
   envelope += "\"payload\":" + payload + ",";
   envelope += "\"error_code\":" + errorCodeJSON + ",";
   envelope += "\"error_message\":" + errorMessageJSON;
   envelope += "}";

   uchar body[];
   int bodyLength = StringToCharArray(envelope, body, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   uchar frame[];
   ArrayResize(frame, bodyLength + 4);
   frame[0] = (uchar)((bodyLength >> 24) & 0xff);
   frame[1] = (uchar)((bodyLength >> 16) & 0xff);
   frame[2] = (uchar)((bodyLength >> 8) & 0xff);
   frame[3] = (uchar)(bodyLength & 0xff);
   for(int i = 0; i < bodyLength; i++)
      frame[i + 4] = body[i];

   SendAll(frame);
}

bool SendAll(uchar &frame[])
{
   int total = ArraySize(frame);
   int sent = 0;
   while(sent < total && !IsStopped())
   {
      int remaining = total - sent;
      uchar chunk[];
      ArrayResize(chunk, remaining);
      ArrayCopy(chunk, frame, 0, sent, remaining);

      ResetLastError();
      int written = SocketSend(g_socket, chunk, (uint)remaining);
      if(written <= 0)
      {
         Print("MT5Research bridge SocketSend failed. sent=", sent, " remaining=", remaining, " error=", GetLastError());
         CloseSocket();
         return false;
      }
      sent += written;
   }
   return sent == total;
}

bool ReadExact(uchar &target[], const int total)
{
   int received = 0;
   uint started = GetTickCount();
   uint timeout = ReadTimeoutMs;
   if(timeout < 1000)
      timeout = 1000;

   while(received < total)
   {
      uchar chunk[];
      int remaining = total - received;
      ArrayResize(chunk, remaining);
      ResetLastError();
      int count = SocketRead(g_socket, chunk, (uint)remaining, 50);
      if(count <= 0)
      {
         if((uint)(GetTickCount() - started) >= timeout)
            return false;
         Sleep(1);
         continue;
      }
      for(int i = 0; i < count; i++)
         target[received + i] = chunk[i];
      received += count;
   }
   return true;
}

string ExtractPayload(const string json)
{
   int key = StringFind(json, "\"payload\"");
   if(key < 0)
      return "";
   int colon = StringFind(json, ":", key);
   if(colon < 0)
      return "";
   int start = colon + 1;
   while(start < StringLen(json) && StringGetCharacter(json, start) <= ' ')
      start++;
   if(start >= StringLen(json) || StringGetCharacter(json, start) != '{')
      return "";

   int depth = 0;
   bool inString = false;
   bool escaping = false;
   for(int i = start; i < StringLen(json); i++)
   {
      ushort ch = StringGetCharacter(json, i);
      if(inString)
      {
         if(escaping)
            escaping = false;
         else if(ch == '\\')
            escaping = true;
         else if(ch == '"')
            inString = false;
      }
      else
      {
         if(ch == '"')
            inString = true;
         else if(ch == '{')
            depth++;
         else if(ch == '}')
         {
            depth--;
            if(depth == 0)
               return StringSubstr(json, start, i - start + 1);
         }
      }
   }
   return "";
}

string JsonStringField(const string json, const string field)
{
   string pattern = "\"" + field + "\":";
   int start = StringFind(json, pattern);
   if(start < 0)
      return "";
   start += StringLen(pattern);
   while(start < StringLen(json) && StringGetCharacter(json, start) <= ' ')
      start++;
   if(start >= StringLen(json) || StringGetCharacter(json, start) != '"')
      return "";
   start++;

   string result = "";
   bool escaping = false;
   for(int i = start; i < StringLen(json); i++)
   {
      ushort ch = StringGetCharacter(json, i);
      if(escaping)
      {
         if(ch == 'n') result += "\n";
         else if(ch == 'r') result += "\r";
         else if(ch == 't') result += "\t";
         else result += ShortToString(ch);
         escaping = false;
      }
      else if(ch == '\\')
         escaping = true;
      else if(ch == '"')
         return result;
      else
         result += ShortToString(ch);
   }
   return "";
}

long JsonLongField(const string json, const string field)
{
   string pattern = "\"" + field + "\":";
   int start = StringFind(json, pattern);
   if(start < 0)
      return 0;
   start += StringLen(pattern);
   while(start < StringLen(json) && StringGetCharacter(json, start) <= ' ')
      start++;

   int end = start;
   while(end < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, end);
      if((ch >= '0' && ch <= '9') || ch == '-')
         end++;
      else
         break;
   }
   return (long)StringToInteger(StringSubstr(json, start, end - start));
}

string PayloadChecksum(const string payload)
{
   uchar bytes[];
   int length = StringToCharArray(payload, bytes, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   ulong hash = 14695981039346656037;
   ulong prime = 1099511628211;
   for(int i = 0; i < length; i++)
   {
      hash = hash ^ (ulong)bytes[i];
      hash = hash * prime;
   }
   return "fnv64:" + ULongToHex(hash);
}

int PayloadByteLength(const string payload)
{
   uchar bytes[];
   return StringToCharArray(payload, bytes, 0, WHOLE_ARRAY, CP_UTF8) - 1;
}

string JsonDouble(const double value, const int digits)
{
   if(!MathIsValidNumber(value))
      return "null";
   return DoubleToString(value, SafeDoubleDigits(digits));
}

string JsonOptionalDouble(const double value, const int digits)
{
   if(!MathIsValidNumber(value) || value == 0.0)
      return "null";
   return DoubleToString(value, SafeDoubleDigits(digits));
}

int SafeDoubleDigits(const int digits)
{
   if(digits < 0)
      return 0;
   return digits;
}

string ULongToHex(ulong value)
{
   string hex = "";
   string chars = "0123456789abcdef";
   for(int i = 0; i < 16; i++)
   {
      int nibble = (int)(value & 0x0f);
      hex = StringSubstr(chars, nibble, 1) + hex;
      value = value >> 4;
   }
   return hex;
}

string JsonEscape(const string value)
{
   string result = "";
   for(int i = 0; i < StringLen(value); i++)
   {
      ushort ch = StringGetCharacter(value, i);
      if(ch == '"') result += "\\\"";
      else if(ch == '\\') result += "\\\\";
      else if(ch == '\n') result += "\\n";
      else if(ch == '\r') result += "\\r";
      else if(ch == '\t') result += "\\t";
      else result += ShortToString(ch);
   }
   return result;
}
