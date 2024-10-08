import socket
import threading
import pickle

cache_id = None
log_file = None
# 데이터 서버에 연결하여 데이터를 요청하는 클라이언트 역할
def request_to_data_server(cache_server_socket, data_socket):
    while True:
        request = cache_server_socket.recv(1024)
        if not request:
            break
        print(f"Cache Server requesting file from Data Server: {request.decode()}")
        data_socket.send(request)  # 데이터 서버로 요청 전달
        data_response = data_socket.recv(1024)
        cache_server_socket.send(data_response)  # 클라이언트로 응답 전달
    cache_server_socket.close()

# 클라이언트 요청 처리
def handle_client(client_socket, address, data_socket):
    print(f"Client {address} connected to Cache Server.")
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            print(f"Received from Client {address}: {data.decode()}")
            # 캐시에 파일이 없으면 데이터 서버에 요청
            threading.Thread(target=connect_to_data_server, args=(client_socket, data_socket)).start()
        except:
            break
    print(f"Client {address} disconnected.")
    client_socket.close()

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
