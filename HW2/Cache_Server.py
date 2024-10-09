import socket
import threading
import pickle

cache_id = None
file_queue = []
file_queue_lock = threading.Lock()

# 클라이언트 요청 처리
# 클라이언트로부터 파일 요청을 받고, 해당 파일을 데이터 서버에 요청하여 전달

def send_result(client_socket, result):
    try:
        data_to_send = pickle.dumps(result)
        client_socket.sendall(data_to_send)
    except Exception as e:
        print(f"Error sending result to client: {e}")

# 데이터 서버에 파일 요청 및 응답 처리

def request_to_data_server(data_socket, requested_file):
    global cache_id
    try:
        # 데이터 서버에 요청 전달
        send_result(data_socket, (cache_id, requested_file))
        print(f"Cache Server {cache_id} requested file {requested_file} from Data Server.")
        
        # 데이터 서버로부터 응답 수신
        response_data = data_socket.recv(1024)
        if response_data:
            download_time = pickle.loads(response_data)
            return download_time
    except Exception as e:
        print(f"Error during communication with Data Server: {e}")
    return None

# 클라이언트로부터 파일 요청을 수신하고 처리하는 함수

def handle_client(client_socket, data_socket):
    try:
        while True:
            # 클라이언트로부터 요청 수신
            client_request = client_socket.recv(1024)
            if not client_request:
                break

            request_data = pickle.loads(client_request)
            requested_file = request_data
            print(f"Cache Server {cache_id} received file request: {requested_file}")
            
            # 파일 요청 처리 (캐시 확인 후 데이터 서버로 요청)
            with file_queue_lock:
                if requested_file in file_queue:
                    download_time = requested_file / 3000  # 3Mbps 다운로드 속도 적용
                    print(f"Cache hit for file {requested_file} in Cache Server {cache_id}.")
                else:
                    download_time = request_to_data_server(data_socket, requested_file)
                    if download_time is not None:
                        if sum(file_queue) + requested_file <= 200000:  # 캐시 용량 확인 (200MB 제한)
                            file_queue.append(requested_file)
                        else:
                            print("Cache capacity exceeded. File not added.")

            # 클라이언트에게 다운로드 시간 전달
            if download_time is not None:
                send_result(client_socket, download_time)
                print(f"Sent download time {download_time} to client from Cache Server {cache_id}.")
            else:
                print(f"Failed to retrieve file {requested_file} from Data Server.")

    except Exception as e:
        print(f"Error handling client in Cache Server {cache_id}: {e}")
    finally:
        client_socket.close()
        print(f"Client connection closed in Cache Server {cache_id}.")

# 데이터 서버에 연결
def connect_to_data_server():
    global cache_id
    try:
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.connect(('localhost', 10000))  # 데이터 서버에 연결
        cache_id = pickle.loads(data_socket.recv(1024))
        print(f"Cache Server received ID: {cache_id}")
        return data_socket
    except Exception as e:
        print(f"Error connecting to Data Server: {e}")
        return None

# 클라이언트 연결 처리
def cache_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port))
    server_socket.listen(4)  # 4개의 클라이언트 수용 가능
    print(f"Cache Server listening on port {port}...")

    data_socket = connect_to_data_server()  # 데이터 서버와 연결
    if not data_socket:
        print("Failed to connect to Data Server. Shutting down cache server.")
        return

    try:
        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=handle_client, args=(client_socket, data_socket)).start()
    except KeyboardInterrupt:
        print("Shutting down cache server.")
    finally:
        server_socket.close()
        data_socket.close()

if __name__ == "__main__":
    port = int(input("Enter cache server port (20000 or 30000): "))  # 포트 입력을 통해 캐시 서버 실행
    cache_server(port)