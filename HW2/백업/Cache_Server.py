import socket
import pickle
import struct
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import heapq

# 데이터 서버가 전송해준 클락 관리
master_clock = 0
master_clock_lock = threading.Lock()

# 캐시 메모리와 데이터 서버 연결 정보
cache_memory = {}  # 캐시에 저장된 파일 정보를 딕셔너리로 관리
cache_size = 200 * 1024  # 캐시 서버의 최대 용량을 200MB로 설정
current_size = 0  # 캐시 서버의 현재 용량
cache_memory_lock = threading.Lock()

log_queue = []
log_queue_lock = threading.Lock()

#log_file = open("Data Server.txt", "w")
#def log_write(event):
#    log_file.write(fs"{event}\n")
#    print(event)
#    log_file.flush()

# 클라이언트 또는 데이터 서버로 데이터를 전송하는 함수
def recv_data(sock, size, max_retries=3):
    received_data = b""
    retries = 0

    while len(received_data) < size and retries < max_retries:
        try:
            packet = sock.recv(min(size - len(received_data), 4096))
            if not packet:
                raise ConnectionError("Connection closed unexpectedly")
            received_data += packet
        except (ConnectionResetError, socket.timeout) as e:
            retries += 1
            print(f"Connection error while receiving data: {e}. Retrying... ({retries}/{max_retries})")
            continue
        except Exception as e:
            retries += 1
            print(f"Error receiving data: {e}. Retrying... ({retries}/{max_retries})")
            continue

    if len(received_data) == size:
        return received_data
    else:
        print(f"Failed to receive data after {max_retries} retries.")
        return None

def send_data(sock, data, clock, send_clock, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            send_to_data = pickle.dumps((clock, send_clock, data))
            data_size = len(send_to_data)
            sock.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
            sock.sendall(send_to_data)  # 실제 데이터 전송
            print(f"Sent data: {data}")
            return  # 성공적으로 보냈을 경우 함수 종료
        except (ConnectionResetError, socket.timeout) as e:
            retries += 1
            print(f"Connection error: {e}. Retrying... ({retries}/{max_retries})")
        except Exception as e:
            retries += 1
            print(f"Error while sending data: {e}. Retrying... ({retries}/{max_retries})")
    print(f"Failed to send data after {max_retries} retries.")


# 데이터 서버로 파일을 요청하는 함수
def request_file_from_data_server(data_socket, file_number):
    global master_clock
    send_data(data_socket, file_number, 0, 0)  # 데이터 서버에 파일 번호를 요청
    #print(f"Requested file {file_number} from Data Server")
    with log_queue_lock and master_clock_lock:
        log_message = f"Clock [{master_clock:.2f}]  Requested file {file_number} from Data Server."
        heapq.heappush(log_queue, (master_clock, log_message))

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, data_socket, client_id):
    global current_size, cache_size, master_clock
    max_retries = 3  # 재시도 횟수
    retries = 0

    try:
        while True:
            # 클라이언트로부터 파일 번호 요청을 수신
            packed_size = recv_data(client_socket, 8, max_retries)  # 데이터 크기 수신
            if not packed_size:
                print(f"Client {client_id} disconnected.")
                break  # 클라이언트 연결이 종료되면 루프를 빠져나감
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = recv_data(client_socket, data_size, max_retries)  # 데이터를 수신
            if received_data is None:
                print(f"Failed to receive file request from client {client_id} after {max_retries} retries.")
                return

            # 받은 데이터를 역직렬화하여 파일 번호를 얻음
            try:
                recieved_master_clock, recieved_clock, file_number = pickle.loads(received_data)
            except (pickle.UnpicklingError, EOFError, ValueError) as e:
                print(f"Error unpickling data from client {client_id}: {e}")
                return

            with log_queue_lock and master_clock_lock:
                log_message = f"Clock [{master_clock:.2f}]  Client {client_id} requested file {file_number}."
                heapq.heappush(log_queue, (master_clock, log_message))

            download_time = file_number / 3072

            # 캐시 메모리에서 파일을 찾음 (캐시 히트 또는 미스)
            if file_number in cache_memory:
                # 캐시에 파일이 있으면 클라이언트에 파일 전송
                with master_clock_lock:
                    send_data(client_socket, cache_memory[file_number], master_clock, master_clock + download_time, max_retries)
                with log_queue_lock and master_clock_lock:
                    log_message = f"Clock [{master_clock:.2f}]  Cache hit: Sent file {file_number} to client."
                    heapq.heappush(log_queue, (master_clock, log_message))
            else:
                # 캐시 비우기 및 데이터 서버로부터 파일 요청
                with log_queue_lock and master_clock_lock:
                    log_message = f"Clock [{master_clock:.2f}]  Cache miss: Retrieved and sent file {file_number}."
                    heapq.heappush(log_queue, (master_clock, log_message))
                file_data_size = len(str(file_number))  # 파일 번호를 기준으로 크기 가정

                while current_size + file_data_size > cache_size and cache_memory:
                    with cache_memory_lock:
                        remove_file_number, remove_file_data = random.choice(list(cache_memory.items()))
                        del cache_memory[remove_file_number]
                        current_size -= len(str(remove_file_data))  # 실제 파일 크기만큼 용량 감소
                        with log_queue_lock and master_clock_lock:
                            log_message = f"Clock [{master_clock:.2f}]  Removed file {remove_file_number} from cache to make space."
                            heapq.heappush(log_queue, (master_clock, log_message))

                # 데이터 서버에 파일 요청
                request_file_from_data_server(data_socket, file_number)

                # 데이터 서버로부터 파일을 수신한 후 캐시에 저장하고 클라이언트에 전송
                packed_size = recv_data(data_socket, 8, max_retries)  # 데이터 크기 수신
                if not packed_size:
                    print(f"Failed to receive file size from data server for file {file_number}")
                    return
                data_size = struct.unpack('Q', packed_size)[0]
                received_data = recv_data(data_socket, data_size, max_retries)  # 데이터를 수신
                if received_data is None:
                    print(f"Failed to receive file data from data server for file {file_number}")
                    return
                try:
                    recrecieved_master_clock, recieved_clock, file_data = pickle.loads(received_data)
                except (pickle.UnpicklingError, EOFError, ValueError) as e:
                    print(f"Error unpickling data from data server: {e}")
                    return

                with cache_memory_lock:
                    cache_memory[file_number] = file_data
                    current_size += len(str(file_data))  # 실제 파일 크기 추가
                    with log_queue_lock and master_clock_lock:
                        log_message = f"Clock [{master_clock:.2f}]  Added file {file_number} to cache."
                        heapq.heappush(log_queue, (master_clock, log_message))

                with master_clock_lock:
                    master_clock = recieved_clock
                    send_data(client_socket, file_data, master_clock, master_clock + download_time, max_retries)
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")



# 데이터 서버에 연결하는 함수
def connect_to_data_server():
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
    print("Connected to Data Server")
    return data_socket

# 캐시 서버를 실행하는 함수
def cache_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port))  # 캐시 서버 포트 설정
    server_socket.listen(4)  # 최대 4개의 클라이언트 연결 대기

    print(f"Cache Server {port} started, waiting for connections...")

    # 데이터 서버와 연결 설정
    data_socket = connect_to_data_server()

    # 스레드 풀을 사용하여 클라이언트 요청을 병렬로 처리
    with ThreadPoolExecutor(max_workers=4) as client_executor:
        client_id = 0
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 연결 대기
            client_id += 1
            print(f"Connected to Client {client_id}")
            
            # 클라이언트 요청 처리
            client_executor.submit(handle_client, client_socket, data_socket, client_id)

def print_log():
    global master_clock
    heapq.heappush(log_queue, (0.000001, "Clock [0]  All connections complete. Start operation."))
    while True:
        # if not log_queue: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
        #     with clock_list_lock:
        #       final_clock = max(clock_list)
        #     print(f"Clock [{final_clock}]  finish")
        #     # 최종로그 내용 추가 필요
        #     return
        if log_queue and log_queue[0][0] <= master_clock:  # master_clock보다 작거나 같다면
            with log_queue_lock:
                if log_queue and log_queue[0][0] <= master_clock:
                    _, log_message = heapq.heappop(log_queue)  # 해당 값을 pop
                    print(log_message)
                    # 파일에 출력하는 코드 필요

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트 입력 받기

    # log 출력 스레드
    log_thread = threading.Thread(target=print_log)
    log_thread.start()

    cache_server(port)
