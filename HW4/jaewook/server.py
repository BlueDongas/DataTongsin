import socket
import queue
import threading

class Server:
    def __init__(self, host, port, max_clients):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = {} # 연결된 클라이언트 소켓과 ID를 저장할 리스트
        self.client_id = 1
        
        self.request_queue =queue.Queue()
        self.response_queue = queue.Queue()
        self.semaphore = threading.Semaphore(1)
        
    def notify_clients_ready(self):
        # 모든 클라이언트에게 준비 완료 신호를 전송
        for client_id, client_socket in self.connected_clients.items():
            client_socket.sendall("READY".encode())
            
    def wait_for_all_clients(self):
        # 모든 클라이언트가 연결될 때까지 기다림
        while len(self.connected_clients) < self.max_clients:
            client_socket, address = self.server_socket.accept()
            print(f"Client{self.client_id} is connected")

            # 클라이언트에게 ID 전송
            client_socket.sendall(str(self.client_id).encode())
            print(f"Send ID to Client{self.client_id}")

            # 연결된 클라이언트 목록에 추가
            self.connected_clients[self.client_id] = client_socket

            # 다음 클라이언트 ID로 증가
            self.client_id += 1

        print("All Client is connecnted. Start Operate.")
        self.notify_clients_ready()
            
    def Server_start(self):
        self.server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen(4)
        print(f"Server is start. Can connect with max {self.max_clients} Client.")

        self.wait_for_all_clients()
    def request_chunk(self):
      print("request")
      
    def send_chunk(self):
      print("send")
        
if __name__ == "__main__":
    server =  Server(host="0.0.0.0",port=6000,max_clients=4)
    server.Server_start()
    
    waiting_thread = threading.Thread(target=server.request_chunk)
    manage_thread = threading.Thread(target=server.send_chunk)

    manage_thread.start()
    waiting_thread.start()

    waiting_thread.join()
    manage_thread.join()
    print("finish")
    input()