import socket
import queue
import threading
import random

class Clock:
    def __init__(self):
        self.master_clock = 0

class Server:
    def __init__(self, host= "127.0.0.1", port = 12345, max_clients=4):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = []  # 연결된 클라이언트 소켓과 ID를 저장할 리스트
        self.client_id = 1
    
    def Server_start(self):
        self.server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen(4)
        print(f"서버가 시작되었습니다. 최대 {self.max_clients}개의 클라이언트와 연결할 수 있습니다.")

    def wait_for_all_clients(self):
        # 모든 클라이언트가 연결될 때까지 기다림
        while len(self.connected_clients) < self.max_clients:
            client_socket, address = self.server_socket.accept()
            print(f"클라이언트가 연결되었습니다: {address}")

            # 클라이언트에게 ID 전송
            client_socket.sendall(str(self.client_id).encode())
            print(f"클라이언트 {self.client_id}에게 ID를 전송했습니다.")

            # 연결된 클라이언트 목록에 추가
            self.connected_clients.append((client_socket, self.client_id))

            # 다음 클라이언트 ID로 증가
            self.client_id += 1

        print("모든 클라이언트가 연결되었습니다. 데이터 전송을 시작합니다.")

    def notify_clients_ready(self):
        # 모든 클라이언트에게 준비 완료 신호를 전송
        for client_socket, client_id in self.connected_clients:
            client_socket.sendall("READY".encode())


    def communicate_with_all_clients(self):
        # 모든 클라이언트와 무작위 순서로 통신을 시작
        while self.connected_clients:
            # 매번 무작위로 클라이언트 순서를 섞음
            random.shuffle(self.connected_clients)
            
            for client_socket, client_id in self.connected_clients:
                try:
                    # 클라이언트로부터 데이터 수신
                    data = client_socket.recv(1024).decode()
                    if not data:
                        # 클라이언트가 연결을 끊으면 리스트에서 제거
                        self.connected_clients.remove((client_socket, client_id))
                        print(f"클라이언트 {client_id}와의 연결이 종료되었습니다.")
                    else:
                        print(f"클라이언트 {client_id}에서 받은 데이터: {data}")
                except ConnectionResetError:
                    # 클라이언트와의 연결이 끊어졌을 경우 처리
                    self.connected_clients.remove((client_socket, client_id))
                    print(f"클라이언트 {client_id}와의 연결이 비정상적으로 종료되었습니다.")

if __name__ == "__main__":
    server = Server()
    server.Server_start()
    server.wait_for_all_clients()
    server.notify_clients_ready()
    server.communicate_with_all_clients()
    input()