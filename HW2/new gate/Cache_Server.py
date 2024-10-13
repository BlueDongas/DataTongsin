import socket
import pickle
import struct
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import heapq
import time

#파일 변수
cache_file_id = 0
log_file = None
# 데이터 서버가 전송해준 클락 관리
master_clock = 0
master_clock_lock = threading.Lock()

End_count = 0
End_count_Lock = threading.Lock()

# 캐시 메모리와 데이터 서버 연결 정보
cache_memory = {}  # 캐시에 저장된 파일 정보를 딕셔너리로 관리
cache_size = 200 * 1024  # 캐시 서버의 최대 용량을 200MB로 설정
current_size = 0  # 캐시 서버의 현재 용량
cache_memory_lock = threading.Lock()

log_queue = []
log_queue_lock = threading.Lock()

cache_hit_count = 0
cache_miss_count = 0
cache_lock = threading.Lock()

total_data_file_size = 0 # 전체 파일 크기 변수
total_data_size_lock = threading.Lock()
total_client_file_size = 0 # 전체 파일 크기 변수
total_client_size_lock = threading.Lock()
#log_file = open("Data Server.txt", "w")
#def log_write(event):
#    log_file.write(fs"{event}\n")
#    print(event)
#    log_file.flush()

# 클라이언트 또는 데이터 서버로 데이터를 전송하는 함수
def send_data(sock, data, clock, send_clock):
    try:
        # 데이터를 직렬화한 후 크기를 먼저 전송하고 데이터를 전송
        send_to_data = pickle.dumps((clock,send_clock,data))
        data_size = len(send_to_data)
        sock.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        sock.sendall(send_to_data)  # 실제 데이터 전송
        print(f"Sent data: {data}")
        #cache -> data : 0 / cache -> client : clock
        # receive_id, master_clock
    except Exception as e:
        print(f"Error while sending data: {e}")

# 데이터 서버로 파일을 요청하는 함수
def request_file_from_data_server(data_socket, file_number):
    global master_clock
    send_data(data_socket, file_number, 0, 0)  # 데이터 서버에 파일 번호를 요청
    #print(f"Requested file {file_number} from Data Server")
    log_message = f"Clock [{master_clock:.2f}]  Requested file {file_number} from Data Server."
    with log_queue_lock:
        heapq.heappush(log_queue, (master_clock, log_message))

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, data_socket, client_id):
    global current_size, cache_size, master_clock,End_count,cache_miss_count,cache_hit_count, total_client_file_size, total_data_file_size
    try:    
        while True:
            # 클라이언트로부터 파일 번호 요청을 수신
            packed_size = client_socket.recv(8)
            if not packed_size:
                break  # 클라이언트 연결이 종료되면 루프를 빠져나감
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)
                received_data += packet

            # 받은 데이터를 역직렬화하여 파일 번호를 얻음
            recieved_master_clock,recieved_client_clock, file_number = pickle.loads(received_data)
            if recieved_client_clock !=0:
                with End_count_Lock:
                    End_count+=1
                    if End_count ==4:
                        send_data(data_socket,0,0,1)
                        # print("JongRyo")
                        input("Enter preess Any Key")
                    return
            # print(f"Client {client_id} requested file {file_number}")
            log_message = f"Clock [{master_clock:.2f}]  Client {client_id} requested file {file_number}."
            with log_queue_lock:
                heapq.heappush(log_queue, (master_clock, log_message))

            download_time = file_number / 3072
            with total_client_size_lock:
                total_client_file_size+=file_number
            # 캐시 메모리에서 파일을 찾음 (캐시 히트 또는 미스)
            if file_number in cache_memory:
                # 캐시에 파일이 있으면 클라이언트에 파일 전송
                with master_clock_lock:
                    send_clock = master_clock + download_time
                    send_data(client_socket, cache_memory[file_number], master_clock, send_clock)

                # print(f"Cache hit: Sent file {file_number} to client")
                log_message = f"Clock [{master_clock:.2f}]  Cache hit: Sent file {file_number} to client."
                with cache_lock:
                    cache_hit_count+=1
                with log_queue_lock:
                    heapq.heappush(log_queue, (master_clock, log_message))
            else:
                # 캐시 비우기
                # print(f"Cache miss: Retrieved and sent file {file_number}")
                log_message = f"Clock [{master_clock:.2f}]  Cache miss: Retrieved and sent file {file_number}."

                cache_miss_count+=1
                with log_queue_lock:
                    heapq.heappush(log_queue, (master_clock, log_message))
                file_data_size = len(str(file_number))  # 파일 번호를 기준으로 크기 가정 (실제 파일 크기에 맞게 변경 필요)
                while current_size + file_data_size > cache_size and cache_memory:
                    with cache_memory_lock:
                        remove_file_number, remove_file_data = random.choice(list(cache_memory.items()))
                        del cache_memory[remove_file_number]
                        current_size -= len(str(remove_file_data))  # 실제 파일 크기만큼 용량 감소
                        # print(f"Removed file {remove_file_number} from cache to make space")
                        log_message = f"Clock [{master_clock:.2f}]  Removed file {remove_file_number} from cache to make space."
                        with log_queue_lock:
                            heapq.heappush(log_queue, (master_clock, log_message))

                # 캐시에 파일이 없으면 데이터 서버에 요청
                request_file_from_data_server(data_socket, file_number)
                with total_data_size_lock:
                    total_data_file_size += file_number
                # 데이터 서버로부터 파일을 수신한 후 캐시에 저장하고 클라이언트에 전송
                packed_size = data_socket.recv(8)
                data_size = struct.unpack('Q', packed_size)[0]
                received_data = b""
                while len(received_data) < data_size:
                    packet = data_socket.recv(4096)
                    received_data += packet
                recrecieved_master_clock,recieved_data_clock,file_data = pickle.loads(received_data)
                with cache_memory_lock:
                    cache_memory[file_number] = file_data
                current_size += len(str(file_data))  # 실제 파일 크기 추가
                log_message = f"Clock [{master_clock:.2f}]  Added file {file_number} to cache."
                with log_queue_lock:
                    heapq.heappush(log_queue, (master_clock, log_message))
                
                with master_clock_lock:
                    master_clock = recieved_data_clock
                    send_clock = master_clock + download_time
                    send_data(client_socket, file_data, master_clock, send_clock)
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")

# 데이터 서버에 연결하는 함수
def connect_to_data_server():
    global cache_file_id,log_file
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
    
    #cache_file_id = pickle.loads(data_socket.recv(1024))
    #log_file = open(f"Cache Server{cache_file_id}.txt", "w")
    print(f"Connected to Data Server")
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
    global master_clock, total_data_file_size, total_client_file_size
    while True:
        if End_count == 4: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
            time.sleep(2)
            while log_queue:
                _, log_message = heapq.heappop(log_queue)
                print(log_message)
            print(f"Final clock [{master_clock}]")
            print(f"cache_hit : {cache_hit_count}")
            print(f"cache_miss : {cache_miss_count}")
            print(f"Average receive download speed : {total_data_file_size/master_clock/1024:.02f}Mbps")
            print(f"Average send speed : {total_client_file_size/master_clock/1024:.02f}Mbps")
            # 최종로그 내용 추가 필요
            return
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
