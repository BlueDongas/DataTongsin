import socket
import threading
import pickle
import queue
import heapq
import struct
import time
# 가상 파일 목록 및 크기 (1 ~ 10000번 파일, 크기는 파일 번호에 비례)
virtual_files = {i: i for i in range(1, 10001)}
connect_count =0
client_connected_condition = threading.Condition()

client_queue = {} #클라이언트별 요청 큐를 관리하기 위한 딕셔너리
cache_queue = {} #캐시 별 요청 큐를 관리하기 위한 딕셔너리
client_queue_lock = threading.Lock()
cache_queue_lock = threading.Lock()
log_queue_lock = threading.Lock()
clock_list_lock = threading.Lock()
socket_lock = threading.Lock()

client_lock = threading.Lock()
file_number_queue = queue.Queue()
file_number_queue_semaphore = threading.Semaphore(4)

master_clock = 0  # 전역 마스터 클락
clock_list = dict(Cache1=0, Cache2=0, Client1=0, Client2=0, Client3=0, Client4=0)

log_queue = []  # 우선순위 큐 (heapq를 사용하여 관리)
#log_file = open("Data Server.txt", "w")

def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()

def send_data(socket, data): # 바꾼 함수 
    try:
        send_to_data = pickle.dumps((0,data)) # clock 보내는 걸로 바꿔야함
        data_size = len(send_to_data)

        with socket_lock:
            socket.sendall(struct.pack('Q', data_size))
            socket.sendall(send_to_data)
    except Exception as e:
        print(f"Error while sending data: {e}")
# 캐시한테 요청받은 파일을 처리하는 함수
def receive_cache_file(cache_socket, cache_id):
    if cache_id not in cache_queue:
        cache_queue[cache_id] = queue.Queue()
    while True:
        try:
            packet_size = b""
            while len(packet_size)<8:
                packet = cache_socket.recv(8-len(packet_size))
                if not packet:
                    print("No size information received. Closing connection.")
                    break
                packet_size += packet

            data_size = struct.unpack('Q', packet_size)[0]
            
            receive_data = b""
            while len(receive_data)<data_size:
                packet = cache_socket.recv(4096)
                if not packet:
                    print("Connection closed while receiveing data.")
                    break
                receive_data+=packet

            # 파일 번호를 받아서 해당 파일을 전송하는 로직
            received_cache_id,file_number = pickle.loads(receive_data)

            # cache_id를 "cache1", "cache2" 형식으로 가공
            # cache_name = f"Cache{received_cache_id}"
            
            
            with cache_queue_lock:
                if received_cache_id in cache_queue:
                    cache_queue[received_cache_id].put(file_number)
                    print(f"Cache{received_cache_id} requested file {file_number}.")
                
            # log_message = f"Clock [{clock:.2f}]  {cache_name} requested file {file_number}."
            # with log_queue_lock:
            #     heapq.heappush(log_queue, (clock, log_message))
            # 클락 + 속도 구하는 로직 추가

        except Exception as e:
            print(f"Error sending file to client {cache_id}: {e}")
            break

def send_cache_file(cache_socket, cache_id):
    while True:
        try:
            with cache_queue_lock:
                if cache_id not in cache_queue:
                    continue
                if cache_queue[cache_id].empty():
                    continue
                file_number = cache_queue[cache_id].get() 

            # download_time = file_number / 2048
            # cache_name = f"Cache{cache_id}"
            # with clock_list_lock:
            #     clock_list[cache_name] += download_time
            #     global master_clock
            #     master_clock = min(clock_list.values())
            
            # 보낼 데이터에 클락정보 필요
            
            send_data(cache_socket,file_number)
            
            print(f"Send file {file_number} to Cache{cache_id}")
            # log_message = f"Clock [{clock_list[cache_name]:.2f}]  Send file {file_number} to {cache_name}."
            # with log_queue_lock:
            #     heapq.heappush(log_queue, (clock_list[cache_name], log_message))
        except Exception as e:
            print(f"Error sending file to cache {cache_id}: {e}")
            break

# 캐시 서버가 요청한 파일을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    try:
        cache_socket.sendall(pickle.dumps(cache_id))  # 해당 클라이언트한테 고유 ID 전달
        receive_cache_thread = threading.Thread(target=receive_cache_file, args=(cache_socket, cache_id))
        send_cache_thread = threading.Thread(target=send_cache_file, args=(cache_socket, cache_id))

        receive_cache_thread.start()
        send_cache_thread.start()

        receive_cache_thread.join()
        send_cache_thread.join()
    finally:
        cache_socket.close()



# 클라이언트가 요청한 파일을 처리하는 함수
def receive_file(client_socket, client_id):
    if client_id not in client_queue:
        with client_queue_lock:
            client_queue[client_id] = queue.Queue()
    while True:
        try:
            packet_size = client_socket.recv(8)
            if not packet_size:
                print(f"No size information received from client {client_id}.")
                return
            
            data_size = struct.unpack('Q',packet_size)[0]
            # 파일 번호를 받아서 해당 파일을 전송하는 로직

            receive_file = b""
            while len(receive_file)<data_size:
                packet = client_socket.recv(4096)
                if not packet:
                    print("Connection closed while receiveing data.")
                    break
                receive_file+=packet

            received_client_id,file_number = pickle.loads(receive_file)

            # client_id를 "Client1", "Client2", "Client3", "Client4" 형식으로 가공
            # client_name = f"Client{received_client_id}"

            with client_queue_lock:
                if received_client_id in client_queue:
                    client_queue[received_client_id].put(file_number)
                    print(f"Client {client_id} requested file {file_number}.")
                    # log_message = f"Clock [{clock:.2f}]  {client_name} requested file {file_number}."
                    # with log_queue_lock:
                    #     heapq.heappush(log_queue, (clock, log_message))
            # 클락 + 속도 구하는 로직 추가
        except Exception as e:
            print(f"Error receive file to client {client_id}: {e}")
            break

def send_file(client_socket, client_id):
    while True:
        try:
            if client_id not in client_queue:
                continue
            
            with client_queue_lock:
                file_number = client_queue[client_id].get()

            # download_time = file_number / 1024
            # client_name = f"Client{client_id}"
            # with clock_list_lock:
            #     clock_list[client_name] += download_time
            #     global master_clock
            #     master_clock = min(clock_list.values())


            # 클락정보도 같이 포함해서 보내야함
            send_to_data = pickle.dumps((file_number))
            data_size = len(send_to_data)

            with socket_lock:
                client_socket.sendall(struct.pack('Q', data_size))
                client_socket.sendall(send_to_data)
            
            print(f"Send file {file_number} to {client_id}")
            # log_message = f"Clock [{clock_list[client_name]:.2f}]  Send file {file_number} to {client_name}."
            # with log_queue_lock:
            #     heapq.heappush(log_queue, (clock_list[client_name], log_message))
        except Exception as e:
            print(f"Error sending file to client {client_id}: {e}")
            break
        
# 클라이언트가 요청한 파일을 처리하는 함수
def handle_client(client_socket, client_id):
    global connect_count
    try:
        with client_connected_condition:
            connect_count += 1
            if connect_count < 4:
                client_connected_condition.wait()  # 나머지 클라이언트 연결을 대기
            else:
                client_connected_condition.notify_all()  # 모든 클라이언트 연결 후 시작 신호 전달
                print("start")
        
        # 클라이언트 작업 시작
        client_socket.sendall(pickle.dumps(client_id))  # 고유 ID 전달
        time.sleep(3)
        client_socket.sendall(pickle.dumps("start"))  # 시작 신호 전달

        receive_thread = threading.Thread(target=receive_file, args=(client_socket, client_id))
        send_thread = threading.Thread(target=send_file, args=(client_socket, client_id))

        receive_thread.start()
        send_thread.start()

        receive_thread.join()
        send_thread.join()
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")
    finally:
        client_socket.close()

# 캐시 서버를 처리하는 함수
def accept_cache(server_socket, num_cache):
    cache_sockets = []

    for cache_id in range(1, num_cache + 1):
        cache_socket, addr = server_socket.accept()
        print(f"Cache Server {cache_id} connected.")
        cache_sockets.append((cache_socket, addr))
        cache_socket.settimeout(100)

    threads = []
    for cache_id, (cache_socket, addr) in enumerate(cache_sockets, 1):
        thread = threading.Thread(target=handle_cache, args=(cache_socket, cache_id))
        threads.append(thread)
        thread.start()

    return threads     

# 클라이언트를 처리하는 함수
def accept_clients(server_socket, num_clients):
    client_sockets = []

    for client_id in range(1, num_clients + 1):
        client_socket, addr = server_socket.accept()
        client_sockets.append((client_socket, addr))
        print(f"Client {client_id} connected.")
        client_socket.settimeout(100)

    threads = []
    for client_id, (client_socket, addr) in enumerate(client_sockets, 1):
        thread = threading.Thread(target=handle_client, args=(client_socket, client_id))
        threads.append(thread)
        thread.start()

    return threads

# def print_log():
#     heapq.heappush(log_queue, (0.000001, "Clock [0]  All connections complete. Start operation."))
#     while True:
#         if not log_queue: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
#             with clock_list_lock:
#               final_clock = max(clock_list.value())
#             print(f"Clock [{final_clock}]  finish")
#             return
#         if log_queue and log_queue[0][0] <= master_clock:  # master_clock보다 작거나 같다면
#             with log_queue_lock:
#                 if log_queue and log_queue[0][0] <= master_clock:
#                     _, log_message = heapq.heappop(log_queue)  # 해당 값을 pop
#                     print(log_message)


def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 10000))  
    server_socket.listen(6)  # 캐시 서버 2개 + 클라이언트 4개 수용
    cache_threads = accept_cache(server_socket, 2)
    client_threads = accept_clients(server_socket, 4)

    # log 출력 스레드
    # log_thread = threading.Thread(target=print_log)
    # log_thread.start()
    
    for thread in cache_threads:
        thread.join()
        
    for thread in client_threads:
        thread.join()
    
    server_socket.close()
    
if __name__ == "__main__":
    main()
