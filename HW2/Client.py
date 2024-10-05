import socket
import threading
# 다운로드할 파일을 요청하는 함수
def request_file(client_socket):
    print("request_file")
    return 0
# 서버들로부터 정보를 받는 함수
def receive_file(client_socket):
    print("receive_file")
    return 0

def connect_to_server(server_address, server_port, server_name): #client handler
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_address, server_port))
        print(f"Connected to {server_name} on port {server_port}")
        request_thread = threading.Thread(target=request_file, args=(client_socket))
        receive_thread = threading.Thread(target=request_file, args=(client_socket))
        
        request_thread.start()
        receive_thread.start()
        print("good job jaewook")
        request_thread.join()
        receive_thread.join()
    finally:
        client_socket.close()
        print(f"Connection closed for client")

# 클라이언트가 데이터 서버와 2개의 캐시 서버에 각각 연결
def client():
    # 각 서버에 대한 정보를 설정
    servers = [
        ('localhost', 10000, 'Data Server'),
        ('localhost', 20000, 'Cache Server 1'),
        ('localhost', 30000, 'Cache Server 2')
    ]
    
    threads = []
    
    # 각 서버에 대한 스레드를 생성하여 연결을 동시에 수행
    for server_address, server_port, server_name in servers:
        thread = threading.Thread(target=connect_to_server, args=(server_address, server_port, server_name))
        thread.start()
        threads.append(thread)
    
    # 모든 스레드가 종료될 때까지 대기
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    client()
