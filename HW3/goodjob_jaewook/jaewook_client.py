import socket
import time

class Client:
    def __init__(self, host='127.0.0.1', port=12345):
        self.host = host
        self.port = port
        self.client_id = None  # 서버로부터 받은 클라이언트 ID

    def connect_to_server(self):
        # 서버와의 연결 생성
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))
        print("서버에 연결되었습니다.")

        # 서버로부터 클라이언트 ID 수신
        self.client_id = int(self.client_socket.recv(1024).decode())
        print(f"서버로부터 할당된 클라이언트 ID: {self.client_id}")

        # 서버로부터 "READY" 신호 대기
        ready_signal = self.client_socket.recv(1024).decode()
        if ready_signal == "READY":
            print("서버 준비 완료. 데이터 전송 시작.")

    def send_file_contents(self):
        # 클라이언트 ID에 맞는 파일 이름 설정
        filename = f"Expression{self.client_id}.txt"
        
        try:
            # 파일 열기
            with open(filename, 'r') as file:
                # 첫 10줄만 읽어서 서버에 전송
                for i in range(10):
                    line = file.readline().strip()
                    if not line:  # 파일에 더 이상 내용이 없으면 종료
                        break
                    self.client_socket.sendall(line.encode())
                    print(f"{filename}의 내용 전송: {line}")
                    time.sleep(0.1)  # 각 줄 전송 후 약간의 지연을 줌
        except FileNotFoundError:
            print(f"파일을 찾을 수 없습니다: {filename}")

    def disconnect(self):
        self.client_socket.close()
        print("서버와의 연결이 종료되었습니다.")

if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    client.send_file_contents()  # 파일의 첫 10줄 내용을 서버로 전송
    client.disconnect()
    input()

    # def send_file_contents(self):
    #     # 클라이언트 ID에 맞는 파일 이름 설정
    #     filename = f"Expression{self.client_id}.txt"
        
    #     try:
    #         # 파일 열기
    #         with open(filename, 'r') as file:
    #             # 파일의 각 줄을 읽어서 서버에 전송
    #             for line in file:
    #                 line = line.strip()  # 줄 끝의 공백 제거
    #                 self.client_socket.sendall(line.encode())
    #                 print(f"{filename}의 내용 전송: {line}")
    #                 time.sleep(0.1)  # 각 줄 전송 후 약간의 지연을 줌
    #     except FileNotFoundError:
    #         print(f"파일을 찾을 수 없습니다: {filename}")

    # def disconnect(self):
    #     self.client_socket.close()
    #     print("서버와의 연결이 종료되었습니다.")