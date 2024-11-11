import socket
import threading
import os
import time
import queue
import json
import base64

TOTAL_CHUNK = None # maybe 3907 정확하게 파일 크기가 512,000,000 byte 기준
CHUNK_SIZE = 128 * 1024 #128kb

send_event=threading.Event()
receive_event=threading.Event()

send_event.set()

file_chunks = {} # [("A","1"):chunk_data] 형식

class Client:
    def __init__(self, host = "localhost", port = 6000):
        self.host = host
        self.port = port
        self.client_id = None
        self.my_file = None
        self.file_path = None
        self.target_files = None
        
        self.request_queue = queue.Queue() #서버의 요청을 저장하는 큐 [clock,target_client_id,send_file_id,send_chunk_id]

    def get_file_size(self):
        global TOTAL_CHUNK, CHUNK_SIZE
        self.file_path = f"./{self.my_file}.file"
        time.sleep(0.3)
        file_size = os.path.getsize(self.file_path)
        TOTAL_CHUNK = int((file_size+CHUNK_SIZE-1)//CHUNK_SIZE)
        print(f"{TOTAL_CHUNK}")

    def make_file_chunk(self):
        global TOTAL_CHUNK,CHUNK_SIZE
        with open(self.file_path,'rb') as f:
            for chunk_id in range(TOTAL_CHUNK):
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    print("not chunk_data")
                    break
                file_chunks[(self.my_file,str(chunk_id))] = chunk_data

    def connect_to_server(self):
        # 서버와의 연결 생성
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))

        # 서버로부터 클라이언트 ID 수신, 가지고 있는 파일 설정
        self.client_id = int(self.client_socket.recv(1024).decode())
        #open(f"client{self.client_id}_Log.txt","w")
        if self.client_id == 1:
            self.my_file = "A"
            self.target_files = ["B","C","D"]
        elif self.client_id == 2:
            self.my_file = "B"
            self.target_files = ["A","C","D"]
        elif self.client_id == 3:
            self.my_file = "C"
            self.target_files = ["A","B","D"]
        elif self.client_id == 4:
            self.my_file = "D"
            self.target_files = ["A","B","C"]
        print("Connect to Server")
        print(f"Receive ID to Server: {self.client_id}")
        
        self.client_socket.sendall
        ready_signal = self.client_socket.recv(1024).decode()
        if ready_signal == "READY":
            print("Start Send task to Server")

        time.sleep(1)
        self.get_file_size()
        self.make_file_chunk() # file 청크 데이터 key-value 형식으로 분할

    def send_to_server(self):
        for file_id in self.target_files: # B,C,D
            for chunk_id in range(CHUNK_SIZE): #1,2,3,4,5,...3907
                send_event.wait()
                while not self.request_queue.empty(): #요청 받은 파일 먼저 전송
                    clock,target_client_id,send_file_id,send_chunk_id = self.request_queue.get()
                    chunk_data_b64 = base64.b64decode(file_chunks[(send_file_id,send_chunk_id)]).decode('utf-8') #chunk_data base64로 문자열화
                    json_data = {
                        "clock":0,
                        "target_client_id":target_client_id,
                        "file_id":send_file_id,
                        "chunk_id":send_chunk_id,
                        "chunk_data":chunk_data_b64,
                        "flag":"request"
                    }

                    data_to_send = json.dumps(json_data)
                    self.client_socket.sendall(data_to_send.encode())
                    print(f"Clock [{clock}]:Send to server chunk{send_chunk_id} of file{send_file_id}")
                    send_event.clear()
                    receive_event.set()

                if (file_id,str(chunk_id)) not in file_chunks:
                    json_data={
                        "clock":0,
                        "target_client_id":self.client_id,
                        "file_id":file_id,
                        "chunk_id":chunk_id,
                        "chunk_data":"None",
                        "flag":"response"
                    }
                    data_to_send=json.dumps(json_data)
                    self.client_socket.sendall(data_to_send.encode())
                    print(f"Clock [{clock}]:Request to server chunk{chunk_id} of file{file_id}")
            return
    def receive_to_server(self):
        while True:
            receive_event.wait()
            print("receive_chunk")
            return

    def disconnect(self):
        return
    
if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    
    send_thread = threading.Thread(target=client.send_to_server)
    receive_thread = threading.Thread(target=client.receive_to_server)
    
    send_thread.start()
    receive_thread.start()
    
    send_thread.join()
    receive_thread.join()
    print("finish")
    input()