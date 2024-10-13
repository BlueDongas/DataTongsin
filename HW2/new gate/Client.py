import socket
import pickle
import struct
import random
import threading
import time
import heapq
from concurrent.futures import ThreadPoolExecutor

file_counter = 10 # 총 다운 받을 파일 개수 설정
sleep_time = 4

receive_file_count = 0
receive_file_count_lock = threading.Lock()

clock = 0
clock_list = [0,0,0]
master_clock=0
master_clock_lock = threading.Lock()
clock_list_lock = threading.Lock()

log_queue = []
log_queue_lock = threading.Lock()

log_file = None
def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()


# 클라이언트에서 서버로 파일 요청을 보내는 함수
def send_request(client_socket, server_id, file_number,server_type,last_clock): 
    global master_clock
    try:
        with clock_list_lock:
            send_to_data = pickle.dumps((clock_list[server_id],last_clock,file_number))  # 파일 번호를 직렬화
        client_socket.sendall(struct.pack('Q', len(send_to_data)))  # 데이터 크기 전송
        client_socket.sendall(send_to_data)  # 파일 번호 전송

        with log_queue_lock and clock_list_lock: 
            log_massage = f"Clock [{clock_list[server_id]:.2f}]  Request file {file_number} from {server_type}"
            heapq.heappush(log_queue, (clock_list[server_id], log_massage))
            
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
        recrecieved_master_clock,recieved_clock,file_number = pickle.loads(file_data)  # 파일 번호를 역직렬화
        return file_number, recieved_clock
    return None

# 클라이언트에서 서버로 파일 요청을 처리하는 함수
def client_task(server_address, port, rq_file_list, server_type,file_list):
    global receive_file_count, file_counter, sleep_time, master_clock

    server_id = None

    if server_type == 'Cache Server 1':
        server_id = 0
    elif server_type == 'Cache Server 2':
        server_id = 1
    elif server_type == 'Data Server':
        server_id = 2

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_address, port))  # 서버에 연결

    print(f"Clock [{clock_list[server_id]}]  Connected to {server_type} on port {port}")

    # 요청할 파일 번호 리스트에 대해 서버에 요청
    while True:
        if receive_file_count == file_counter:
            print(f"All task complete. receive file counter : {receive_file_count}.")
            with clock_list_lock:
                master_clock = max(clock_list)
            send_request(client_socket, server_id, file_number, server_type, master_clock)
            break
        if not rq_file_list:
            continue
        file_number = file_list[0]
        try:
            if file_number == rq_file_list[0]:
                rq_file_list.pop(0)
                file_list.pop(0)
            else:
                continue

            time.sleep(sleep_time)

            send_request(client_socket, server_id, file_number,server_type, 0)
            received_file, receive_clock = receive_file(client_socket)
            if received_file:
                with receive_file_count_lock:
                    receive_file_count+=1
                with clock_list_lock:
                    clock_list[server_id] = receive_clock
                    master_clock = min(clock_list)
                with log_queue_lock and clock_list_lock: 
                    log_message = f"Clock [{clock_list[server_id]:.2f}]  Receive file from {server_type} : {received_file}"
                    heapq.heappush(log_queue,(clock_list[server_id], log_message))
        except Exception as e:
            print(f"Failed to send request file{file_number} to {server_type} because {e}")

def print_log():
    global master_clock
    heapq.heappush(log_queue, (0.0000001, "Clock [0]  All connections complete. Start operation."))
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


# 클라이언트가 동시에 데이터 서버와 캐시 서버에 파일 요청을 보내는 함수
def client():
    global file_counter

    data_server_address = ('localhost', 10000)  # 데이터 서버 주소
    cache_server1_address = ('localhost', 20000)  # 캐시 서버 1 주소
    cache_server2_address = ('localhost', 30000)  # 캐시 서버 2 주소

    file_list = []
    Odd_list = []
    Even_list = []
    Data_request_list = []

    odd_cache_sum = 0
    even_cache_sum = 0
    data_sum = 0

    for _ in range(file_counter): #테스트용 나중에 1000개로 수정
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

    print(Even_list)
    print(Odd_list)
    print(Data_request_list)

    # 스레드 풀을 이용해 캐시 서버와 데이터 서버에 동시에 요청
    with ThreadPoolExecutor(max_workers=100) as executor:
        # 캐시 서버 1에 요청
        executor.submit(client_task, cache_server1_address[0], cache_server1_address[1], Even_list, 'Cache Server 1',file_list)

        # 캐시 서버 2에 요청
        executor.submit(client_task, cache_server2_address[0], cache_server2_address[1], Odd_list, 'Cache Server 2',file_list)

        # 데이터 서버에 요청
        executor.submit(client_task, data_server_address[0], data_server_address[1], Data_request_list, 'Data Server',file_list)

if __name__ == "__main__":

    log_thread = threading.Thread(target=print_log)
    log_thread.start()
    client()
    input()  # 프로그램이 종료되지 않도록 입력 대기
