import socket
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import json
import heapq
import struct
import os

TOTAL_CHUNK = None
BUFFER_SIZE = 1024*150
SLEEP_TIME = 0

class Server:
    def __init__(self, host, port, max_clients):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = {} # 연결된 클라이언트 소켓과 ID를 저장할 리스트
        self.client_id = 1
        
        self.request_queue = [[], [], [], [], []] # [clock,target_client_id,file_id,chunk_id] 
        self.response_queue = [[], [], [], [], []] # [clock,target_client_id,file_id,chunk_id,chunk_data]
        self.semaphore = threading.Semaphore(1)

        self.executor = ThreadPoolExecutor()

        self.chunk_owner_data = {} #각 클라이언트가 보유한 청크 정보 저장 ex) {("B",chunk_id):client_id} <- 중복의 경우 스케줄링 알고리즘에 따라 교체

        self.clock_list = [0, 0, 0, 0, 0] # 0은 master_clock
        self.clock_list_lock = [threading.Lock() for _ in range(5)]
        self.log_queue = []

        self.receive_event_list = [threading.Event() for _ in range(5)]
        self.send_event_list = [threading.Event() for _ in range(5)]

        self.is_complete = [False, False, False, False, False] # client 요청이 끝났는지 여부 max 4
        self.request_end = [False, False, False, False, False] # 요청큐가 비었고 client 요청이 끝났는지
        self.response_end = False # 요청 큐가 비었고 client 요청이 끝났으며 응답도 끝남.

    def init_chunk_owner_data(self):
        global TOTAL_CHUNK
        file_list = ['A','B','C','D']
        client_id = 1
        for file in file_list:
            for chunk_id in range(TOTAL_CHUNK):
                self.chunk_owner_data[(file,chunk_id)] = [client_id]
            client_id+=1
    
    def notify_clients_ready(self):
        global TOTAL_CHUNK
        # 모든 클라이언트에게 준비 완료 신호를 전송
        for client_id, client_socket in self.connected_clients.items():
            data = client_socket.recv(4)
            TOTAL_CHUNK = struct.unpack('!I', data)[0]

        self.init_chunk_owner_data() #각 client 초기 청크 보유 정보 초기화

        for client_id, client_socket in self.connected_clients.items():# 준비 완료 신호 전송
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
        buffer = ""
        while True:
            if all(self.is_complete[1:]) and self.response_end: #종료
                print("All task is complete")
                break
            
            data = client_socket.recv(BUFFER_SIZE).decode()
            buffer += data
            while '\n' in buffer:
                line,buffer = buffer.split('\n',1)
                try:
                    json_data = json.loads(line)
                    flag = json_data.get('flag')
                    
                    if flag == "complete":
                        self.is_complete[client_id] = True
                        print(f"receive complete client{client_id} flag  {self.is_complete[client_id]}")

                    if flag == "request":
                        _ = json_data.get('clock')
                        target_client_id = json_data.get('target_client_id')
                        file_id = json_data.get('file_id')
                        chunk_id = json_data.get('chunk_id')
                        chunk_data = json_data.get('chunk_data')

                        with self.clock_list_lock[client_id]:
                            self.clock_list[client_id] = round(self.clock_list[client_id] + (5 + client_id) / 10, 1)
                            self.clock_list[0] = min(self.clock_list[1:4])
                            clock = self.clock_list[client_id]

                        destination_client = min(self.chunk_owner_data[(file_id,chunk_id)], key=lambda x: self.clock_list[x])

                        heapq.heappush(self.request_queue[destination_client], (clock,destination_client,target_client_id,file_id,chunk_id))
                        #print(f"Clock [{clock}]:Receive [Request] file[{file_id}] chunk[{chunk_id}]")
                        log_message = f"Clock [{clock}]:Receive [Request] From [Client{client_id}] file[{file_id}] chunk[{chunk_id}]"
                        heapq.heappush(self.log_queue, (clock, log_message))

                    elif flag =="response":
                        _ = json_data.get('clock')
                        target_client_id = json_data.get('target_client_id')
                        file_id = json_data.get('file_id')
                        chunk_id = json_data.get('chunk_id')
                        chunk_data = json_data.get('chunk_data')

                        with self.clock_list_lock[client_id]:
                            self.clock_list[client_id] += 5 + client_id
                            self.clock_list[0] = min(self.clock_list[1:4])
                            clock = self.clock_list[client_id]
                        heapq.heappush(self.response_queue[target_client_id], (clock,target_client_id,file_id,chunk_id,chunk_data))
                        # print(f"Clock [{clock}]:Receive [Data] file[{file_id}] chunk[{chunk_id}]")
                        log_message = f"Clock [{clock}]:Receive [Data] from [Client{client_id}] file[{file_id}] chunk[{chunk_id}]"
                        heapq.heappush(self.log_queue, (clock, log_message))

                except json.JSONDecodeError as e:
                    print(f"Error to Receive from {client_id} : {e}")

                # print(f"debug : send_event client{client_id} set")
                self.send_event_list[client_id].set()
                # print(f"debug : receive_event client{client_id} wait")
                self.receive_event_list[client_id].wait()
                self.receive_event_list[client_id].clear()

    def send_to_client(self,client_id,client_socket):
        # print(f"debug : send_event client{client_id} wait")
        self.send_event_list[client_id].wait()
        self.send_event_list[client_id].clear()
        while True:
            if not self.response_queue[client_id] and self.request_end[0]:
                self.response_end = True
                print("Response is complete")
                json_data = {"flag":"complete"}
                data_to_send = json.dumps(json_data)+'\n'
                for client_id,client_socket in client_items:
                    client_socket.sendall(data_to_send.encode())
                return
            elif self.response_queue[client_id]:
                clock,target_client_id,file_id,chunk_id,chunk_data = heapq.heappop(self.response_queue[client_id])
                destination_socket = self.connected_clients[target_client_id]

                with self.clock_list_lock[target_client_id]:
                    self.clock_list[target_client_id] = max(self.clock_list[target_client_id], clock)
                    clock = self.clock_list[target_client_id]
                    self.clock_list[target_client_id] += 5 + target_client_id
                    receive_clock= self.clock_list[target_client_id]

                json_data = {
                    "clock":receive_clock,
                    "target_client_id":"None",
                    "file_id":file_id,
                    "chunk_id":chunk_id,
                    "chunk_data":chunk_data,
                    "flag":"response"
                }
                data_to_send = json.dumps(json_data)+'\n'
                with self.semaphore:
                    destination_socket = self.connected_clients[target_client_id]
                    destination_socket.sendall(data_to_send.encode())
                # print(f"Clock [{clock}]:Send [Data] to [client{target_client_id}] file[{file_id}] chunk[{chunk_id}] data")
                log_message = f"Clock [{clock}]:Send [Data] to [client{target_client_id}] file[{file_id}] chunk[{chunk_id}] data"
                heapq.heappush(self.log_queue, (clock, log_message))

                self.clock_list[0] = min(self.clock_list[1:4])

                self.chunk_owner_data[(file_id, chunk_id)].append(client_id)
                self.chunk_owner_data[(file_id, chunk_id)].sort()

                # print(f"debug : receive_event client{client_id} set")
                self.receive_event_list[client_id].set()
                # print(f"debug : send_event client{client_id} wait")
                self.send_event_list[client_id].wait()
                self.send_event_list[client_id].clear()
                time.sleep(SLEEP_TIME)
            elif not self.request_queue[client_id] and self.is_complete[0]:
                self.request_end[client_id] = True

            elif self.request_queue[client_id]:
                clock,destination_client,target_client_id,file_id,chunk_id = heapq.heappop(self.request_queue[client_id])
                
                with self.clock_list_lock[destination_client]:
                    self.clock_list[destination_client] = max(self.clock_list[destination_client], clock)
                    clock = self.clock_list[destination_client]
                    self.clock_list[destination_client] = round(self.clock_list[destination_client] + (5 + destination_client) / 10, 1)
                    receive_clock = self.clock_list[destination_client]

                json_data = {
                    "clock":receive_clock,
                    "target_client_id":target_client_id,
                    "file_id":file_id,
                    "chunk_id":chunk_id,
                    "chunk_data":"None",
                    "flag":"request"
                }
                
                data_to_send = json.dumps(json_data)+'\n'
                with self.semaphore:
                    destination_socket = self.connected_clients[destination_client]
                    destination_socket.sendall(data_to_send.encode())
                # print(f"Clock [{clock}]:Send [Request] to [client{destination_client}] file[{file_id}] chunk[{chunk_id}] data")
                log_message = f"Clock [{clock}]:Send [Request] to [client{destination_client}] file[{file_id}] chunk[{chunk_id}] data"
                heapq.heappush(self.log_queue, (clock, log_message))

                self.clock_list[0] = min(self.clock_list[1:4])
                # print(f"debug : receive_event client{client_id} set")
                self.receive_event_list[client_id].set()
                # print(f"debug : send_event client{client_id} wait")
                self.send_event_list[client_id].wait()
                self.send_event_list[client_id].clear()
                time.sleep(SLEEP_TIME)

    def handle_client(self,client_id,client_socket):
        receive_thread = threading.Thread(target=server.receive_to_client,args=(client_id,client_socket))
        response_thread = threading.Thread(target=server.send_to_client,args=(client_id,client_socket))
        receive_thread.start()
        response_thread.start()

    def print_log(self):
        while True:
            
            if all(self.is_complete[1:]):
                self.is_complete[0] = True

            if all(self.request_end[1:]):
                self.request_end[0] = True

            if self.response_end: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
                while self.log_queue:
                    _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                    print(log_message)
                # 최종로그 내용 추가 필요
                return
            
            if self.log_queue:
                while self.log_queue[0][0] <= self.clock_list[0] - 20:  # master_clock - 10 보다 작거나 같다면
                    _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                    print(log_message)
            
            
if __name__ == "__main__":
    server =  Server(host="0.0.0.0",port=6000,max_clients=4)
    server.Server_start()
    log_thread = threading.Thread(target=server.print_log)

    log_thread.start()
    client_items = list(server.connected_clients.items())
    server_thread=[]
    for client_id,client_socket in client_items:
        handle_client_thread = threading.Thread(target=server.handle_client,args=(client_id,client_socket))
        handle_client_thread.start()
        server_thread.append(handle_client_thread)
    
    for thread in server_thread:
        thread.join()
    log_thread.join()

    for client_id, client_socket in server.connected_clients.items():
        client_socket.close()
    server.server_socket.close()
    
    input("All task is finish Press Enter Any key")  # 프로그램이 종료되지 않도록 입력 대기
    os._exit(0)