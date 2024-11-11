import socket
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import json
import heapq

BUFFER_SIZE = 1024*150
class Server:
    def __init__(self, host, port, max_clients):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = {} # 연결된 클라이언트 소켓과 ID를 저장할 리스트
        self.client_id = 1
        
        self.request_queue =queue.Queue() # [clock,target_client_id,file_id,chunk_id] 
        self.response_queue = queue.Queue() # [clock,target_client_id,file_id,chunk_id,chunk_data]
        self.semaphore = threading.Semaphore(1)

        self.executor = ThreadPoolExecutor()

        self.chunk_owner_data = {} #각 클라이언트가 보유한 청크 정보 저장 ex) {("B",chunk_id):client_id} <- 중복의 경우 스케줄링 알고리즘에 따라 교체
        
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

    def receive_to_client(self,client_id,client_socket):
        while True:
            data = client_socket.recv(BUFFER_SIZE).decode()
            for line in data.splitlines():
                json_data = json.loads(line)

                clock = json_data.get('clock')
                target_client_id = json_data.get('target_client_id')
                file_id = json_data.get('file_id')
                chunk_id = json_data.get('chunk_id')
                chunk_data = json_data.get('chunk_data')
                flag = json_data.get('flag')

                if flag == "request":
                    self.request_queue.put([clock,target_client_id,file_id,chunk_id])
                elif flag =="response":
                    self.response_queue.put([clock,target_client_id,file_id,chunk_id,chunk_data])

        print("request")    
      
    def send_to_client(self,client_id,client_socket):
        print("send")

    def handle_client(self,client_id,client_socket):
        receive_thread = threading.Thread(target=server.receive_to_client,args=(client_id,client_socket))
        response_thread = threading.Thread(target=server.send_to_client,args=(client_id,client_socket))
        receive_thread.start()
        response_thread.start()
            
            
if __name__ == "__main__":
    server =  Server(host="0.0.0.0",port=6000,max_clients=4)
    server.Server_start()

    client_items = list(server.connected_clients.items())
    server_thread=[]
    for client_id,client_socket in client_items:
        handle_client_thread = threading.Thread(target=server.handle_client,args=(client_id,client_socket))
        handle_client_thread.start()
        server_thread.append(handle_client_thread)
    
    for thread in server_thread:
        thread.join()
    
    server.server_socket.close
    print("finish")
    input()