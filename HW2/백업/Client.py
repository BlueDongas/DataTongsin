import socket
import pickle
import struct
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

file_list = [] #다운받을 리스트
file_list_lock = threading.Lock()

file_getsu = 1000 # 총 다운 받을 파일 개수 설정
sleep_time = 2.5

receive_file_count = 0
receive_file_count_lock = threading.Lock()
clock = 0
master_clock=0

log_file = None
def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()


# 클라이언트에서 서버로 파일 요청을 보내는 함수
def send_request(client_socket, file_number, server_type, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            send_to_data = pickle.dumps((master_clock, clock, file_number))  # 파일 번호를 직렬화
            client_socket.sendall(struct.pack('Q', len(send_to_data)))  # 데이터 크기 전송
            client_socket.sendall(send_to_data)  # 파일 번호 전송
            print(f"Requested file {file_number} to {server_type}")
            return  # 요청 성공 시 함수 종료
        except Exception as e:
            retries += 1
            print(f"Error sending request for file {file_number} to {server_type}: {e}. Retrying... ({retries}/{max_retries})")
            continue
    print(f"Failed to send request for file {file_number} after {max_retries} retries.")

def recv_data(sock, size):
    received_data = b""
    while len(received_data) < size:
        try:
            packet = sock.recv(min(size - len(received_data), 4096))
            if not packet:
                raise ConnectionError("Connection closed unexpectedly")
            received_data += packet
        except (ConnectionResetError, socket.timeout) as e:
            print(f"Connection error while receiving data: {e}")
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            continue
    return received_data if len(received_data) == size else None  # 정확한 크기 검증

# 서버로부터 파일을 수신하는 함수 (재시도 로직 추가)
def receive_file(client_socket, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            packed_size = recv_data(client_socket, 8)  # 데이터 크기를 먼저 받음
            if packed_size:
                data_size = struct.unpack('Q', packed_size)[0]
                file_data = recv_data(client_socket, data_size)  # 데이터를 수신
                if file_data:
                    recrecieved_master_clock, recieved_clock, file_number = pickle.loads(file_data)  # 파일 번호를 역직렬화
                    return file_number  # 파일 번호 반환
            return None  # 데이터가 없으면 None 반환
        except Exception as e:
            retries += 1
            print(f"Error receiving file: {e}. Retrying... ({retries}/{max_retries})")
    print(f"Failed to receive file after {max_retries} retries.")
    return None

# 클라이언트에서 서버로 파일 요청을 처리하는 함수 (변경 없음)
def client_task(server_address, port, rq_file_list, server_type):
    global receive_file_count, file_getsu, sleep_time

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_address, port))  # 서버에 연결
    print(f"Connected to {server_type} on port {port}")

    while True:
        if receive_file_count == file_getsu:
            print(f"All task complete. receive file getsu : {receive_file_count}.")
            break
        if not rq_file_list:
            continue
        with file_list_lock:
            file_number = file_list[0]

        try:
            if file_number == rq_file_list[0]:
                rq_file_list.pop(0)
                with file_list_lock:
                    file_list.pop(0)
            else:
                continue

            time.sleep(sleep_time)

            send_request(client_socket, file_number, server_type)  # 재시도 로직 포함
            received_file = receive_file(client_socket)  # 재시도 로직 포함
            if received_file:
                print(f"Received file from {server_type}: {received_file}")
                with receive_file_count_lock:
                    receive_file_count += 1
        except Exception as e:
            print(f"Failed to process request for file {file_number} to {server_type}: {e}")

# 클라이언트가 동시에 데이터 서버와 캐시 서버에 파일 요청을 보내는 함수 (변경 없음)
def client():
    global file_getsu
    data_server_address = ('localhost', 10000)  # 데이터 서버 주소
    cache_server1_address = ('localhost', 20000)  # 캐시 서버 1 주소
    cache_server2_address = ('localhost', 30000)  # 캐시 서버 2 주소

    Odd_list = []
    Even_list = []
    Data_request_list = []

    odd_cache_sum = 0
    even_cache_sum = 0
    data_sum = 0

    for _ in range(file_getsu):  # 테스트용, 나중에 1000개로 수정
        file_number = random.randint(1, 10000)

        if file_number % 2 == 0:
            if even_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Even_list.append(file_number)
                even_cache_sum += file_number
        else:
            if odd_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Odd_list.append(file_number)
                odd_cache_sum += file_number
        file_list.append(file_number)

    print(Even_list)
    print(Odd_list)
    print(Data_request_list)

    # 스레드 풀을 이용해 캐시 서버와 데이터 서버에 동시에 요청
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 캐시 서버 1에 요청
        executor.submit(client_task, cache_server1_address[0], cache_server1_address[1], Even_list, 'Cache Server 1')

        # 캐시 서버 2에 요청
        executor.submit(client_task, cache_server2_address[0], cache_server2_address[1], Odd_list, 'Cache Server 2')

        # 데이터 서버에 요청
        executor.submit(client_task, data_server_address[0], data_server_address[1], Data_request_list, 'Data Server')

if __name__ == "__main__":
    client()
    input()  # 프로그램이 종료되지 않도록 입력 대
