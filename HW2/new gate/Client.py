import socket
import pickle
import struct
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

file_list = [] #다운받을 리스트
file_list_lock = threading.Lock()

# 클라이언트에서 서버로 파일 요청을 보내는 함수
def send_request(client_socket, file_number,server_type):
    try:
        data = pickle.dumps(file_number)  # 파일 번호를 직렬화
        client_socket.sendall(struct.pack('Q', len(data)))  # 데이터 크기 전송
        client_socket.sendall(data)  # 파일 번호 전송
        print(f"Requested file {file_number} to {server_type}")
    except Exception as e:
        print(f"Error sending request for file {file_number} to {server_type}: {e}")

# 서버로부터 파일을 수신하는 함수
def receive_file(client_socket):
    packed_size = client_socket.recv(8)  # 데이터 크기를 먼저 받음
    if packed_size:
        data_size = struct.unpack('Q', packed_size)[0]
        file_data = b""
        while len(file_data) < data_size:
            packet = client_socket.recv(4096)  # 데이터를 받음
            file_data += packet
        file_number = pickle.loads(file_data)  # 파일 번호를 역직렬화
        return file_number
    return None

# 클라이언트에서 서버로 파일 요청을 처리하는 함수
def client_task(server_address, port, rq_file_list, server_type):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_address, port))  # 서버에 연결
    print(f"Connected to {server_type} on port {port}")

    # 요청할 파일 번호 리스트에 대해 서버에 요청

    while True:
        with file_list_lock:
            file_number = file_list[0]
        if not rq_file_list:
            continue
        try:
            if file_number == rq_file_list[0]:
                with file_list_lock:
                    file_list.pop(0)
                rq_file_list.pop(0)
                time.sleep(1)
                send_request(client_socket,file_number,server_type)        
        except Exception as e:
            print(f"Feiled to send request file{file_number} to {server_type}")
        finally: # 종료 
            with file_list_lock:
                if not file_list:
                    print(f"All task complete {server_type}")
                    break
                
        
    # for file_number in rq_file_list:
    #     send_request(client_socket, file_number)

    #     # 서버로부터 파일 수신
    #     received_file = receive_file(client_socket)
    #     if received_file:
    #         print(f"Received file from {server_type}: {received_file}")

# 클라이언트가 동시에 데이터 서버와 캐시 서버에 파일 요청을 보내는 함수
def client():
    file_numbers = random.randint(1,10000)  # 1~10000 범위의 파일 번호 중 10개 선택
    data_server_address = ('localhost', 10000)  # 데이터 서버 주소
    cache_server1_address = ('localhost', 20000)  # 캐시 서버 1 주소
    cache_server2_address = ('localhost', 30000)  # 캐시 서버 2 주소

    Odd_list = []
    Even_list = []
    Data_request_list = []

    odd_cache_sum = 0
    even_cache_sum = 0
    data_sum = 0

    for _ in range(10): #테스트용 나중에 1000개로 수정
        file_number = random.randint(1,10000)

        if file_number % 2 == 0:
            if even_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Even_list.append(file_number)
                even_cache_sum += file_number
        else : 
            if odd_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else:
                Odd_list.append(file_number)
                odd_cache_sum += file_number
        file_list.append(file_number)

    # 스레드 풀을 이용해 캐시 서버와 데이터 서버에 동시에 요청
    with ThreadPoolExecutor(max_workers=100) as executor:
        # 캐시 서버 1에 요청
        executor.submit(client_task, cache_server1_address[0], cache_server1_address[1], Even_list, 'Cache Server 1')

        # 캐시 서버 2에 요청
        executor.submit(client_task, cache_server2_address[0], cache_server2_address[1], Odd_list, 'Cache Server 2')

        # 데이터 서버에 요청
        executor.submit(client_task, data_server_address[0], data_server_address[1], Data_request_list, 'Data Server')

if __name__ == "__main__":
    client()
    input()  # 프로그램이 종료되지 않도록 입력 대기
