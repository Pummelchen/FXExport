#property strict
#property version   "1.000"
#property description "MT5Research localhost history bridge. Swift owns validation, storage, checkpoints, verification, and repair."

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
   Print("MT5Research HistoryBridgeEA initialized. Waiting to connect to Swift at ", SwiftHost, ":", SwiftPort);
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
      SendOK(requestId, command, "{\"bridge_name\":\"HistoryBridgeEA\",\"bridge_version\":\"0.1\",\"schema_version\":1}");
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
   payload += "\"account_login\":" + IntegerToString((long)AccountInfoInteger(ACCOUNT_LOGIN));
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
   string payload = "{";
   payload += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   payload += "\"selected\":" + (selected ? "true" : "false") + ",";
   payload += "\"digits\":" + IntegerToString(digits);
   payload += "}";
   SendOK(requestId, command, payload);
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
   if(safeToExclusive <= fromTs)
   {
      SendOK(requestId, command, "{\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",\"timeframe\":\"M1\",\"rates\":[]}");
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
   string response = "{";
   response += "\"mt5_symbol\":\"" + JsonEscape(symbol) + "\",";
   response += "\"timeframe\":\"M1\",";
   response += "\"rates\":[";

   int emitted = 0;
   for(int i = 0; i < copied && emitted < maxBars; i++)
   {
      long ts = (long)rates[i].time;
      if(ts < fromTs || ts >= safeToExclusive || ts > latestClosed)
         continue;

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
