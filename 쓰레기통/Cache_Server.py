import socket
import threading 
import pickle
import random
import queue
import struct
import time
import heapq

cache_id = None
log_file = None
connect_count = 0
client_connect_condition = threading.Condition()
clock_lock = threading.Lock()
log_queue_lock = threading.Lock()

cache_memory = [] # cache memory
cache_memory_lock = threading.Lock()
client_queue = {} # 클라이언트 별 요청 큐를 관리하기 위한 딕셔너리
client_queue_lock = threading.Lock()

socket_lock = threading.Lock()

cache_size = 200 * 1024
current_size = 0

clock = 0
log_queue = []

# 데이터 서버에 연결하여 데이터를 요청하는 클라이언트 역할
def log_write(event):
    log_file.write(f"{event}\n")
    print(event)
    log_file.flush()
    
def send_data(socket, data): # 바꾼 함수
    global cache_id, clock
    try:
        print(f"cache_server {cache_id} send data {data} to data server")
        # log_message = f"Clock [{clock:.2f}]  cache_server {cache_id} send data {data} to data server."
        # with log_queue_lock:
        #     heapq.heappush(log_queue, (clock, log_message))
        send_data = pickle.dumps((cache_id,data))
        data_size = len(send_data)

        with socket_lock:
            socket.sendall(struct.pack('Q', data_size))
            socket.sendall(send_data)

    except Exception as e:
        print(f"Error while sending data: {e}")

#def send_data(socket,data): 원래 함수
#    global cache_id
#    send_data = pickle.dumps((cache_id,data))
#    socket.sendall(send_data)

def request_to_data_server(data_socket,file_number,client_socket): # 인자로 클락도 받아오는게 안전할수도 있음. 아님말고
    global clock
    # log_message = f"Clock [{clock:.2f}]  request to data server {file_number}."
    # with log_queue_lock:
    #     heapq.heappush(log_queue, (clock, log_message))
    send_data(data_socket,file_number)
    receive_file_from_data_server(data_socket,file_number,client_socket)

    
def receive_file_from_data_server(data_socket,file_number,client_socket):
    global cache_memory, cache_size, current_size, clock

    packed_size = b""
    while len(packed_size) < 8:  # 8바이트를 모두 수신할 때까지 반복
        packet = data_socket.recv(8 - len(packed_size))
        if not packet:
            print("No size information received from data server. Closing connection.")
            return None
        packed_size += packet
    
    data_size = struct.unpack('Q', packed_size)[0]

    data = b""
    while len(data) < data_size:
        packet = data_socket.recv(4096)
        if not packet:
            print("Connection closed while receiving data.")
            return None
        data += packet
    receive_id,receive_data = pickle.loads(data) # receive_clock 추가해야함
    

    # with clock_lock:
    #     clock = receive_clock  
        
    #clock 처리
    print(f"recieve data : {receive_data} from data server.") # 클락받아서 받았다는 로그 출력으로 바꿔야댐
    # log_message = f"Clock [{receive_clock:.2f}]  Receive data."
    # with log_queue_lock:
    #     heapq.heappush(log_queue, (receive_clock, log_message))
    send_data(client_socket,clock) #clock

def receive_file_to_client(client_socket,data_socket):
    global clock
    while True:
        try:
            with socket_lock:
                receive_data = b""
                while len(receive_data) < 8:
                    packet = client_socket.recv(8 - len(receive_data))
                    if not packet:
                        print("No size information received. Closing connection.")
                        return None
                    receive_data += packet
                data_size = struct.unpack('Q', receive_data)[0]
                data = b""
                while len(data) < data_size:
                    packet = client_socket.recv(4096)
                    if not packet:
                        print("Connection closed while receiving data.")
                        return None
                    data += packet

            received_client_id,receive_file = pickle.loads(data) # receive_clock도 받아야함

            if receive_file == "complete":
                print(f"All task complete")
                break
            
            if received_client_id not in client_queue:
                client_queue[received_client_id] = queue.Queue()
            # with clock_lock:
                # clock = receive_clock

            with client_queue_lock:
                if received_client_id in client_queue:
                    client_queue[received_client_id].put(receive_file)
                    print(f"Receive request file {receive_file} to client {received_client_id}")
                    # log_message = f"Clock [{receive_clock:.2f}]  Receive request file {receive_file} to client {received_client_id}."
                    # with log_queue_lock:
                    #     heapq.heappush(log_queue, (receive_clock, log_message))

            if receive_file in cache_memory: # 캐시 히트 
                send_data(client_socket,receive_file) # receive_clock 함께 보내줘야함
                print(f"Cache hit!! send file {receive_file} to client {received_client_id}")
                # log_message = f"Clock [{receive_clock:.2f}]  Cache hit!! send file {receive_file} to client {received_client_id}."
                # with log_queue_lock:
                #     heapq.heappush(log_queue, (receive_clock, log_message))
            else: # 캐시 미스
                print(f"Cache miss.. request file to data server")
                # log_message = f"Clock [{receive_clock:.2f}]  Cache miss.. request file to data server."
                # with log_queue_lock:
                #     heapq.heappush(log_queue, (receive_clock, log_message))
                # receive_clock += receive_file / 3072
                request_to_data_server_thread = threading.Thread(target=request_to_data_server,args=(data_socket,receive_file,client_socket)) # receive_clock 함께 보내줘야함
                request_to_data_server_thread.start()
                # 캐시 메모리에 추가
                while current_size + receive_file > cache_size and cache_memory:
                    # RR 알고리즘 사용해 캐시 메모리 비우기
                    remove_index = random.randint(0, len(cache_memory) - 1)
                    with cache_memory_lock:
                        remove_file = cache_memory.pop(remove_index)
                        current_size -= remove_file
                        print(f"Remove file form cache to make space : {remove_file}")
                    # log_message = f"Clock [{receive_clock:.2f}]  Remove file form cache to make space : {remove_file}"
                    # with log_queue_lock:
                    #     heapq.heappush(log_queue, (receive_clock, log_message))

                # 파일 추가
                with cache_memory_lock:
                    cache_memory.append(receive_file)
                    current_size += receive_file
                    print(f"Added file {receive_file} to cache")
                # log_message = f"Clock [{receive_clock:.2f}]  Added file {file_number} to cache."
                # with log_queue_lock:
                #     heapq.heappush(log_queue, (receive_clock, log_message))
            

        except Exception as e:
            print(f"Error to recive file to client")
            break

# 클라이언트 요청 처리
def handle_client(client_socket, address, data_socket):
    global connect_count
    try:
        with client_connect_condition:
            connect_count+=1
            print(f"Client {connect_count} connected.")
            if connect_count < 4:
                client_connect_condition.wait()
            else:
                client_connect_condition.notify_all()
        
        client_socket.sendall(pickle.dumps("start"))
        time.sleep(3)

        receive_thread = threading.Thread(target=receive_file_to_client,args=(client_socket,data_socket))
        #send_thread = threading.Thread(target=send_file_to_client,args=(client_socket, data))

        receive_thread.start()
        #send_thread.start()

        receive_thread.join()
        #send_thread.join()
    except Exception as e:
        print(f"Error handle_client because {e} ")
    finally: client_socket.close()


# 데이터 서버에 연결
def connect_to_data_server_as_client():
    global cache_id
    global log_file
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
    cache_id = pickle.loads((data_socket.recv(4096)))
    #log_file = open(f"Cache Server{cache_id}.txt", "w")
    return data_socket

# 캐시 서버는 클라이언트에 대해 서버 역할, 데이터 서버에 대해 클라이언트 역할을 수행
def cache_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port)) # 클라이언트 연결
    server_socket.listen(4)  # 4개의 클라이언트 수용
    print(f"Cache Server listening on port {port}...")

    data_socket = connect_to_data_server_as_client()  # 데이터 서버와 연결
    
    try:
        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=handle_client, args=(client_socket, addr, data_socket)).start()
    except KeyboardInterrupt:
        print("Shutting down cache server.")
    finally:
        server_socket.close()
        data_socket.close()

# def print_log():
#     global clock
#     heapq.heappush(log_queue, (0.000001, "Clock [0]  All connections complete. Start operation."))
#     while True:
#         if not log_queue: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
#             with clock_lock:
#                 final_clock = clock
#             print(f"Clock [{final_clock}]  finish")
#             return
#         if log_queue and log_queue[0][0] <= clock:  # master_clock보다 작거나 같다면
#             with log_queue_lock:
#                 if log_queue and log_queue[0][0] <= clock:
#                     _, log_message = heapq.heappop(log_queue)  # 해당 값을 pop
#                     print(log_message)

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트를 입력받아 캐시 서버 실행

    # log 출력 스레드
    # log_thread = threading.Thread(target=print_log)
    # log_thread.start()
    
    cache_server(port)
