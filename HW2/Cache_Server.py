import socket
import threading

# 캐시 서버에서 사용할 파일 캐시(파일 번호와 파일 크기를 저장)
cache = {}
CACHE_LIMIT = 200 * 1024  # 200MB 제한
cache_size = 0

# 캐시 서버에 요청된 파일이 있는지 확인하고, 없으면 데이터 서버에 요청하는 함수
def handle_client(client_socket, data_server_address):
    global cache_size
    try:
        while True:
            # 클라이언트로부터 파일 요청을 받음
            requested_file = client_socket.recv(1024).decode('utf-8')
            if not requested_file:
                break
            requested_file = int(requested_file)
            
            # 캐시에서 파일을 찾음
            if requested_file in cache:
                # 캐시에 파일이 있으면 클라이언트로 전송
                client_socket.sendall(f"Cache hit: Sending file {requested_file}".encode('utf-8'))
            else:
                # 캐시에 파일이 없으면 데이터 서버로 요청
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as data_socket:
                    data_socket.connect(data_server_address)
                    print("handle client")
                    data_socket.sendall(str(requested_file).encode('utf-8'))
                    file_data = data_socket.recv(1024)  # 파일 크기 정보를 받음
                    
                    # 파일을 캐시에 저장할 수 있는지 확인
                    file_size = int(file_data.decode('utf-8').split()[-1])  # 파일 크기 추출
                    if cache_size + file_size <= CACHE_LIMIT:
                        # 캐시에 파일 추가
                        cache[requested_file] = file_size
                        cache_size += file_size
                        client_socket.sendall(f"Cache miss: Fetched file {requested_file} from Data Server".encode('utf-8'))
                    else:
                        client_socket.sendall(f"Cache miss: Cache full, unable to store file {requested_file}".encode('utf-8'))
    finally:
        client_socket.close()

# 캐시 서버를 실행하는 함수
def start_cache_server(cache_port, data_server_address):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', cache_port))
    server_socket.listen(4)  # 최대 4개의 클라이언트 처리 가능
    
    print(f"Cache Server listening on port {cache_port}")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Client connected from {addr}")
        threading.Thread(target=handle_client, args=(client_socket, data_server_address)).start()

if __name__ == "__main__":
    # 데이터 서버의 주소와 포트
    data_server_address = ('localhost', 10000)
    
    # 첫 번째 캐시 서버는 포트 20000에서 실행
    threading.Thread(target=start_cache_server, args=(20000, data_server_address)).start()
    
    # 두 번째 캐시 서버는 포트 30000에서 실행
    threading.Thread(target=start_cache_server, args=(30000, data_server_address)).start()
