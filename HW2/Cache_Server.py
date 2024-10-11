import socket
import threading 
import pickle
import random

cache_id = None
log_file = None
cache_memory = [] # cache memory
cache_size = 200 * 1024
current_size = 0
# 데이터 서버에 연결하여 데이터를 요청하는 클라이언트 역할


def send_data(socket,data):
    global cache_id
    send_data = pickle.dumps(data)
    socket.sendall(cache_id,send_data)

def request_to_data_server(data_socket,file_number,client_socket):
    print(f"request to data server {file_number}")
    send_data(data_socket,file_number)
    threading.Thread(target=receive_file_from_data_server,args=(data_socket,file_number,client_socket)).start()

def receive_file_from_data_server(data_socket,file_number,client_socket):
    global cache_memory, cache_size, current_size
    received_clock = data_socket.recv(1024)
    clock = pickle.loads(received_clock)
    #clock 처리
    print(f"recieve data : {clock}") # 클락받아서 받았다는 로그 출력으로 바꿔야댐
    send_data(client_socket,clock) #clock

    # 캐시 메모리에 추가
    while current_size + file_number > cache_size and cache_memory:
        # RR 알고리즘 사용해 캐시 메모리 비우기
        remove_index = random.randint(0, len(cache_memory) - 1)
        remove_file = cache_memory.pop(remove_index)
        current_size -= remove_file
        print(f"Remove file form cache to make space : {remove_file}")

    # 파일 추가
    cache_memory.append(file_number)
    current_size += file_number
    print(f"Added file {file_number} to cache")
            


def receive_file_to_client(client_socket,data_socket):
    while True:
        try:
            receive_data = client_socket.recv(1024)
            receive_file = pickle.loads(receive_data)
            if receive_file == "complete":
                print(f"All task complete")
                break
            # 캐시 메모리와 비교 후 추가 로직 필요

            if receive_file in cache_memory: # 캐시 히트 
                send_data(client_socket,receive_file)
                print(f"Cache hit!! send file {receive_file} to client")
            else: # 캐시 미스
                print(f"Cache miss.. request file to data server")
                request_to_data_server(data_socket,receive_file,client_socket)

        except Exception as e:
            print(f"Error to recive file to client")
            break

def send_file_to_client(client_socket,send_data):
    return 0


# 클라이언트 요청 처리
def handle_client(client_socket, address, data_socket):
    try:
        receive_thread = threading.Thread(target=receive_file_to_client,args=(client_socket,data_socket))
        #send_thread = threading.Thread(target=send_file_to_client,args=(client_socket, data))

        receive_thread.start()
        #send_thread.start()

        receive_thread.join()
        #send_thread.join()
    except Exception as e:
        print(f"Error handle_clientbecause {e} ")

def handle_data():
    return 0
# 데이터 서버에 연결
def connect_to_data_server_as_client():
    global cache_id
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
    cache_id = pickle.loads((data_socket.recv(1024)))
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

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트를 입력받아 캐시 서버 실행
    cache_server(port)
