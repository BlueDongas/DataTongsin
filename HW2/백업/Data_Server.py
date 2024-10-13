import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor

# 가상의 파일 목록을 저장 (1~10000 파일 번호와 파일 내용)
virtual_files = {i: i for i in range(1, 10001)}  # 파일 번호와 내용이 동일한 가상 파일

# 클라이언트에게 파일 데이터를 전송하는 함수
def send_data(client_socket, data):
    try:
        send_data = pickle.dumps(data)  # 데이터를 직렬화
        data_size = len(send_data)
        client_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        client_socket.sendall(send_data)  # 데이터 전송
        print(f"Sent file {data} to client")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, client_id):
    try:
        while True:
            packed_size = client_socket.recv(8)  # 데이터 크기 수신
            if not packed_size:
                break  # 클라이언트 연결이 종료되면 루프 탈출
            data_size = struct.unpack('Q', packed_size)[0]
            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)  # 데이터를 수신
                received_data += packet

            file_number = pickle.loads(received_data)  # 파일 번호를 역직렬화
            if file_number in virtual_files:
                send_data(client_socket, virtual_files[file_number])  # 요청된 파일 전송
                print(f"Client {client_id} requested file {file_number}")
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")

# 데이터 서버를 실행하는 메인 함수
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 10000))  # 데이터 서버 포트 바인딩
    server_socket.listen(5)  # 최대 5개의 클라이언트 연결 대기

    print("Data Server started, waiting for connections...")

    # 스레드 풀을 사용하여 클라이언트 요청을 병렬로 처리
    with ThreadPoolExecutor(max_workers=5) as executor:  # 최대 5개의 클라이언트 동시 처리
        client_id = 0
        while True:
            client_socket, addr = server_socket.accept()  # 클라이언트 연결 대기
            client_id += 1
            print(f"Connected to Client {addr}")
            executor.submit(handle_client, client_socket, client_id)  # 클라이언트 요청 처리

    server_socket.close()

if __name__ == "__main__":
    main()
