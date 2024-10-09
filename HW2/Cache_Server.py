import socket
import threading
import pickle

cache_id = None
log_file = None
cache_memory = [] # cache memory
# 데이터 서버에 연결하여 데이터를 요청하는 클라이언트 역할


def send_data(socket,data):
    send_data = pickle.dumps(data)
    socket.sendall(send_data)

def request_to_data_server(data_socket,file_number,client_socket):
    print(f"request to data server {file_number}")
    send_data(data_socket,file_number)
    threading.Thread(target=receive_file_from_data_server,args=(data_socket,file_number,client_socket)).start()

def receive_file_from_data_server(data_socket,file_number,client_socket):
    received_clock = data_socket.recv(1024)
    clock = pickle.load(received_clock)
    #clock 처리
    print(f"recieve data : {clock}") # 클락받아서 받았다는 로그 출력으로 바꿔야댐
    send_data(client_socket,clock) #clock

def receive_file_to_client(client_socket,data_socket):
    while True:
        try:
            receive_data = client_socket.recv(1024)
            receive_file = pickle.loads(receive_data)
            if receive_file == "complete":
                print(f"All task complete")
                break
            #캐시 메모리와 비교 후 추가 로직 필요
            #캐시 히트 continue 추가해야댐
            send_data(client_socket,receive_file/3000)
            print(f"Cache hit!! send file {receive_file} to client")

            # 캐시 메모리에 없는 경우
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
    finally:
        client_socket.close()
        data_socket.close()

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
