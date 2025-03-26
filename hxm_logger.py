import serial
import csv
from dataclasses import dataclass, field
import time
import traceback # Добавим для отладки ошибок

# Конфигурация
MAX_16BIT = 0xFFFF            # 65535
MAX_8BIT = 0xFF               # 255
TIMESTAMP_RESOLUTION = 1/1024   # Один тик = 1/1024 секунды
DISTANCE_COUNTER_MAX_RAW = 4095 # Максимальное значение raw distance (0-4095)
DISTANCE_COUNTER_ROLLOVER = DISTANCE_COUNTER_MAX_RAW + 1 # 4096 - значение для прибавления к offset при переполнении

def crc8(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x8C
            else:
                crc >>= 1
    return crc

@dataclass
class REventData:
    """Структура для хранения R-события и данных из пакета."""
    corrected_timestamp: int
    distance: float = 0.0 # Общая дистанция на момент пакета (метры)
    speed: float = 0.0    # Мгновенная скорость на момент пакета (м/с)
    # Можно добавить еще поля из пакета при необходимости, например, heart_rate
    # heart_rate: int = 0

@dataclass
class HxMProcessingState:
    """Состояние, необходимое для корректной сшивки данных."""
    # --- Состояние для R-событий ---
    last_processed_heartbeat_num: int = -1
    last_processed_corrected_timestamp: int = -1
    current_timestamp_offset: int = 0

    # --- Состояние для Distance ---
    last_distance_raw: int = -1
    distance_offset: int = 0 # В единицах raw (1/16 метра)

    # --- Состояние для других счетчиков (информационно) ---
    last_packet_heartbeat_num: int = -1

class HxMPacketParser:
    PACKET_SIZE = 60
    STX = 0x02
    ETX = 0x03
    MSG_ID = 0x26
    DLC_EXPECTED = 0x37

    def __init__(self):
        self.buffer = bytearray()

    def append_data(self, data: bytes):
        self.buffer.extend(data)

    def resync_buffer(self):
        idx = self.buffer.find(self.STX)
        if idx > 0:
            print(f"Resync: Discarding {idx} bytes before STX.")
            del self.buffer[:idx]
        elif idx < 0:
            if len(self.buffer) > self.PACKET_SIZE * 2:
                 print(f"Resync: STX not found in {len(self.buffer)} bytes, discarding oldest {self.PACKET_SIZE} bytes.")
                 del self.buffer[:self.PACKET_SIZE]
            else:
                 pass # Ждем еще данных

    def get_next_packet(self) -> bytes | None:
        # (Логика get_next_packet остается без изменений)
        while len(self.buffer) >= self.PACKET_SIZE:
            if self.buffer[0] != self.STX:
                self.resync_buffer()
                continue
            candidate = self.buffer[:self.PACKET_SIZE]
            if candidate[-1] != self.ETX:
                self.buffer.pop(0)
                continue
            if candidate[1] != self.MSG_ID:
                 self.buffer.pop(0)
                 continue
            if candidate[2] != self.DLC_EXPECTED:
                 self.buffer.pop(0)
                 continue
            if not self.validate_packet_crc(candidate):
                self.buffer.pop(0)
                continue
            packet = bytes(candidate)
            del self.buffer[:self.PACKET_SIZE]
            return packet
        return None

    def validate_packet_crc(self, packet: bytes) -> bool:
        # (Логика validate_packet_crc остается без изменений)
        if len(packet) != self.PACKET_SIZE:
            return False
        computed_crc = crc8(packet[3:58])
        expected_crc = packet[58]
        return computed_crc == expected_crc

    def parse_packet(self, packet: bytes, state: HxMProcessingState) -> dict:
        """
        Разбирает пакет, извлекает поля, обрабатывает переполнение Distance
        и ОБНОВЛЯЕТ СОСТОЯНИЕ (state).
        Возвращает словарь с данными, включая 'corrected_distance_meters' и 'speed_mps'.
        """
        result = {}
        result['timestamp_received'] = time.time()
        result['firmware_id'] = int.from_bytes(packet[3:5], 'little')
        result['firmware_version'] = packet[5:7].decode('ascii', errors='replace')
        result['hardware_id'] = int.from_bytes(packet[7:9], 'little')
        result['hardware_version'] = packet[9:11].decode('ascii', errors='replace')
        result['battery'] = packet[11]
        result['heart_rate'] = packet[12] # ЧСС из пакета

        current_packet_heartbeat_num = packet[13]
        result['packet_heartbeat_num'] = current_packet_heartbeat_num

        # R-события (как раньше)
        raw_timestamps_packet_ordered = [int.from_bytes(packet[i:i+2], 'little') for i in range(14, 44, 2)]
        raw_timestamps_chrono = list(reversed(raw_timestamps_packet_ordered))
        result['r_events_packet_chrono'] = raw_timestamps_chrono

        # --- Distance ---
        current_distance_raw = int.from_bytes(packet[50:52], 'little')
        # Проверка и обработка переполнения distance_raw (0-4095)
        if state.last_distance_raw != -1 and current_distance_raw < state.last_distance_raw:
            # Возможно, произошло переполнение. Проверяем, не слишком ли большой скачок назад.
            # Если скачок назад большой (например, с 4090 до 10), то это переполнение.
            # Если скачок маленький (например, с 100 до 95), это может быть шум или остановка/движение назад?
            # Будем считать переполнением любой переход "вниз".
            state.distance_offset += DISTANCE_COUNTER_ROLLOVER
            print(f"Debug: Distance rollover detected! Raw: {current_distance_raw} < Last Raw: {state.last_distance_raw}. New Offset: {state.distance_offset}")

        # Рассчитываем скорректированное расстояние в метрах
        corrected_distance_raw = state.distance_offset + current_distance_raw
        corrected_distance_meters = corrected_distance_raw / 16.0
        result['corrected_distance_meters'] = corrected_distance_meters
        # Обновляем последнее значение raw для следующего пакета
        state.last_distance_raw = current_distance_raw

        # --- Speed ---
        current_speed_raw = int.from_bytes(packet[52:54], 'little')
        # Просто конвертируем в м/с, без обработки переполнения (т.к. не описано)
        speed_mps = current_speed_raw / 256.0
        result['speed_mps'] = speed_mps

        # --- Strides ---
        result['strides'] = packet[54] # Предполагаем байт

        # Обнаружение пропущенных ПАКЕТОВ (информационно)
        missed_packets = 0
        if state.last_packet_heartbeat_num != -1:
            diff = (current_packet_heartbeat_num - state.last_packet_heartbeat_num) & MAX_8BIT
            if diff > 1:
                 missed_packets = diff - 1
        state.last_packet_heartbeat_num = current_packet_heartbeat_num
        result['missed_packets_detected'] = missed_packets

        return result

class HxMDataLogger:
    """
    Логгер данных HxM. Сшивает R-события, используя heartbeat_num,
    и сохраняет связанные данные пакета (distance, speed).
    """
    def __init__(self):
        self.state = HxMProcessingState()
        self.r_events_list: list[REventData] = []
        self.total_events_processed = 0

    def process_new_packet_data(self, packet_data: dict):
        """
        Обрабатывает данные из нового пакета для сшивки R-событий
        и добавления связанных данных (distance, speed).
        """
        packet_hb_num = packet_data.get('packet_heartbeat_num')
        raw_timestamps_chrono = packet_data.get('r_events_packet_chrono', [])
        # Извлекаем рассчитанные distance и speed
        distance_m = packet_data.get('corrected_distance_meters', 0.0)
        speed_mps = packet_data.get('speed_mps', 0.0)
        # hr = packet_data.get('heart_rate', 0) # Если нужно добавить HR в REventData

        if packet_hb_num is None or not raw_timestamps_chrono:
            #print("Warning: Packet data missing heartbeat_num or r_events.")
            return

        # --- Шаг 1: Определить количество НОВЫХ событий ---
        num_new_beats = 0
        if self.state.last_processed_heartbeat_num == -1:
            num_new_beats = len(raw_timestamps_chrono)
        else:
            num_new_beats = (packet_hb_num - self.state.last_processed_heartbeat_num) & MAX_8BIT

        if num_new_beats == 0:
            return

        # --- Шаг 2: Определить, какие таймштампы из пакета обрабатывать ---
        if num_new_beats > len(raw_timestamps_chrono):
            #print(f"Warning: Calculated {num_new_beats} new beats, but packet only contains {len(raw_timestamps_chrono)} timestamps. Processing available.")
            num_to_process = len(raw_timestamps_chrono)
        else:
            num_to_process = num_new_beats

        new_relevant_timestamps = raw_timestamps_chrono[-num_to_process:]

        # --- Шаг 3: Обработать релевантные таймштампы ---
        last_raw_ts_for_check = -1
        if self.state.last_processed_corrected_timestamp != -1:
            last_raw_ts_for_check = self.state.last_processed_corrected_timestamp % (MAX_16BIT + 1)

        events_added_in_this_batch = 0
        for i, raw_ts in enumerate(new_relevant_timestamps):
            if last_raw_ts_for_check != -1 and raw_ts < last_raw_ts_for_check:
                self.state.current_timestamp_offset += (MAX_16BIT + 1)
                # print(f"Debug: Timestamp rollover detected! Raw TS {raw_ts} < Last Raw TS {last_raw_ts_for_check}. New Offset: {self.state.current_timestamp_offset}")

            corrected_ts = raw_ts + self.state.current_timestamp_offset

            if self.state.last_processed_corrected_timestamp != -1 and corrected_ts <= self.state.last_processed_corrected_timestamp:
                print(f"CRITICAL ERROR: Non-monotonic timestamp! New corrected TS {corrected_ts} <= Last processed TS {self.state.last_processed_corrected_timestamp}. Skipping.")
                continue

            # --- Добавляем событие С ДАННЫМИ ПАКЕТА ---
            event = REventData(
                corrected_timestamp=corrected_ts,
                distance=distance_m,
                speed=speed_mps
                # heart_rate=hr # Если добавили поле HR
            )
            self.r_events_list.append(event)
            events_added_in_this_batch += 1

            # --- Обновляем состояние для СЛЕДУЮЩЕЙ итерации ---
            self.state.last_processed_corrected_timestamp = corrected_ts
            last_raw_ts_for_check = raw_ts

        # --- Шаг 4: Обновить номер последнего обработанного удара ---
        if events_added_in_this_batch > 0:
             self.state.last_processed_heartbeat_num = packet_hb_num
             self.total_events_processed += events_added_in_this_batch

    def save_csv(self, filename: str):
        """Сохраняет накопленные R-события и связанные данные в CSV."""
        if not self.r_events_list:
            print("Нет данных R-событий для сохранения.")
            return

        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Добавляем новые заголовки
                writer.writerow([
                    "Corrected Timestamp (ticks)",
                    "Timestamp (s)",
                    "RR Interval (ms)",
                    "Distance (m)",
                    "Speed (m/s)"
                ])

                monotonic_errors = 0
                # Записываем первое событие (без RR)
                event0 = self.r_events_list[0]
                ts0_sec = event0.corrected_timestamp * TIMESTAMP_RESOLUTION
                writer.writerow([
                    event0.corrected_timestamp,
                    f"{ts0_sec:.6f}",
                    "N/A",
                    f"{event0.distance:.3f}", # Форматируем distance
                    f"{event0.speed:.3f}"     # Форматируем speed
                ])

                # Записываем остальные события
                for i in range(1, len(self.r_events_list)):
                    prev_event = self.r_events_list[i-1]
                    curr_event = self.r_events_list[i]

                    prev_ts_ticks = prev_event.corrected_timestamp
                    curr_ts_ticks = curr_event.corrected_timestamp

                    rr_interval_ticks = curr_ts_ticks - prev_ts_ticks
                    rr_interval_ms = rr_interval_ticks * TIMESTAMP_RESOLUTION * 1000
                    curr_ts_sec = curr_ts_ticks * TIMESTAMP_RESOLUTION

                    rr_interval_ms_str = ""
                    if rr_interval_ticks <= 0:
                         rr_interval_ms_str = f"Error: RR_ticks={rr_interval_ticks}"
                         monotonic_errors += 1
                    else:
                         rr_interval_ms_str = f"{rr_interval_ms:.3f}"

                    writer.writerow([
                        curr_event.corrected_timestamp,
                        f"{curr_ts_sec:.6f}",
                        rr_interval_ms_str,
                        f"{curr_event.distance:.3f}", # Форматируем distance
                        f"{curr_event.speed:.3f}"     # Форматируем speed
                    ])

            print(f"Данные ({len(self.r_events_list)} событий) сохранены в файл {filename}")
            if monotonic_errors > 0:
                print(f"Предупреждение: Обнаружено {monotonic_errors} немонотонных или нулевых RR-интервалов при сохранении.")

        except IOError as e:
            print(f"Ошибка ввода-вывода при сохранении файла {filename}: {e}")
        except Exception as e:
            print(f"Непредвиденная ошибка при сохранении файла: {e}")
            traceback.print_exc() # Печатаем traceback для диагностики

# --- main() функция ---
def main():
    SERIAL_PORT = '/dev/rfcomm0' # Или 'COMx' для Windows
    BAUD_RATE = 115200
    OUTPUT_FILENAME_BASE = "hxm_log_full" # Обновим имя базы

    try:
        ser = serial.Serial(SERIAL_PORT, baudrate=BAUD_RATE, timeout=0.5)
        print(f"Успешное подключение к {SERIAL_PORT} на скорости {BAUD_RATE}")
    except serial.SerialException as e:
        print(f"Ошибка подключения к порту {SERIAL_PORT}: {e}")
        return
    except Exception as e:
        print(f"Непредвиденная ошибка при открытии порта: {e}")
        return

    parser = HxMPacketParser()
    logger = HxMDataLogger()
    packet_count = 0
    last_print_time = time.time()

    try:
        while True:
            bytes_to_read = ser.in_waiting
            if bytes_to_read > 0:
                data = ser.read(bytes_to_read)
                parser.append_data(data)
            else:
                time.sleep(0.01)

            while True:
                packet = parser.get_next_packet()
                if packet is None:
                    break

                packet_count += 1
                try:
                    pkt_data = parser.parse_packet(packet, logger.state)
                    logger.process_new_packet_data(pkt_data)

                    current_time = time.time()
                    if current_time - last_print_time >= 1.0:
                        print(f"--- Packet {packet_count} ---")
                        print(f"  Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        # Выводим distance и speed из pkt_data (они там актуальные для пакета)
                        print(f"  HR: {pkt_data['heart_rate']} | Batt: {pkt_data['battery']}% | Pkt HBNum: {logger.state.last_packet_heartbeat_num} | Events: {logger.total_events_processed}")
                        print(f"  Dist: {pkt_data['corrected_distance_meters']:.2f} m | Speed: {pkt_data['speed_mps']:.2f} m/s") # Добавленный вывод
                        if pkt_data['missed_packets_detected'] > 0:
                           print(f"  Missed Packets Detected: {pkt_data['missed_packets_detected']}")
                        # Показываем последние R-event TS (distance/speed там будут одинаковые для событий из одного пакета)
                        if logger.r_events_list:
                             latest_events = logger.r_events_list[-3:] # Покажем 3 последних
                             print(f"  Latest R-events (TS, Dist, Speed):")
                             for ev in latest_events:
                                 print(f"    TS={ev.corrected_timestamp}, D={ev.distance:.2f}, S={ev.speed:.2f}")
                        else:
                             print("  No R-events processed yet.")
                        last_print_time = current_time

                except Exception as e:
                    print(f"Критическая ошибка при разборе или обработке пакета {packet_count}: {e}")
                    traceback.print_exc()
                    print(f"  Проблемный пакет (hex): {packet.hex()}")

    except KeyboardInterrupt:
        print("\nОстановлено пользователем (Ctrl+C)")
    except serial.SerialException as e:
        print(f"\nОшибка последовательного порта: {e}")
    except Exception as e:
         print(f"\nНепредвиденная ошибка в главном цикле: {e}")
         traceback.print_exc()
    finally:
        if ser and ser.is_open:
            ser.close()
            print("Соединение закрыто.")

        if logger.r_events_list:
            timestamp_str = time.strftime("%Y%m%d_%H%M%S")
            default_filename = f"{OUTPUT_FILENAME_BASE}_{timestamp_str}.csv"
            try:
                filename_input = input(f"Введите имя CSV файла для сохранения (Enter для '{default_filename}'): ")
                filename = filename_input if filename_input else default_filename
                logger.save_csv(filename)
            except EOFError:
                 print(f"Не удалось прочитать имя файла, используется имя по умолчанию: {default_filename}")
                 logger.save_csv(default_filename)
        else:
            print("Нет данных R-событий для сохранения.")

if __name__ == "__main__":
    main()
