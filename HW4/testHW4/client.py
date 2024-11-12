import socket
import threading
import os
import time
import queue
import json
import base64
import struct

TOTAL_CHUNK = None # maybe 3907 정확하게 파일 크기가 512,000,000 byte 기준
CHUNK_SIZE = 128 * 1024 #128kb
BUFFER_SIZE = 1024*150
SLEEP_TIME = 0.1

send_event=threading.Event()
receive_event=threading.Event()

send_event.set()

file_chunks = {} # [("A",1):chunk_data] 형식

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
                file_chunks[(self.my_file,chunk_id)] = chunk_data

    def connect_to_server(self):
        global TOTAL_CHUNK
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
        
        self.get_file_size()
        self.make_file_chunk() # file 청크 데이터 key-value 형식으로 분할
        data = struct.pack('!I', TOTAL_CHUNK)
        self.client_socket.sendall(data)

        ready_signal = self.client_socket.recv(1024).decode()
        if ready_signal == "READY":
            print("Start Send task to Server")

        time.sleep(1)

    def send_to_server(self):
        for file_id in self.target_files: # B,C,D
            for chunk_id in range(TOTAL_CHUNK): #1,2,3,4,5,...3907
                send_event.wait()
                while not self.request_queue.empty(): #요청 받은 파일 먼저 전송
                    clock,target_client_id,send_file_id,send_chunk_id = self.request_queue.get()
                    chunk_data_b64 = base64.b64encode(file_chunks[(send_file_id,send_chunk_id)]).decode('utf-8') #chunk_data base64로 문자열화
                    json_data = {
                        "clock":0,
                        "target_client_id":target_client_id,
                        "file_id":send_file_id,
                        "chunk_id":send_chunk_id,
                        "chunk_data":chunk_data_b64,
                        "flag":"response"
                    }

                    data_to_send = json.dumps(json_data)+"\n"
                    self.client_socket.sendall(data_to_send.encode())
                    time.sleep(SLEEP_TIME)
                    print(f"Clock [0]:Send to server chunk{send_chunk_id+1} of file{send_file_id}")
                    

                if (file_id,chunk_id) not in file_chunks:
                    json_data={
                        "clock":0,
                        "target_client_id":self.client_id,
                        "file_id":file_id, #A
                        "chunk_id":chunk_id,#int 1
                        "chunk_data":"None",
                        "flag":"request"
                    }
                    data_to_send=json.dumps(json_data) + "\n"
                    self.client_socket.sendall(data_to_send.encode())
                    time.sleep(SLEEP_TIME)
                    print(f"Clock [0]:Request to server chunk{chunk_id} of file{file_id}")
    def receive_to_server(self):
        buffer = ""
        while True:
            data = self.client_socket.recv(BUFFER_SIZE).decode()
            buffer += data
            while '\n' in buffer:
                line, buffer = buffer.split('\n',1)
                try:
                    json_data = json.loads(line)

                    clock = json_data.get('clock')
                    target_client_id = json_data.get('target_client_id')
                    file_id = json_data.get('file_id')
                    chunk_id = json_data.get('chunk_id')
                    chunk_data = json_data.get('chunk_data')
                    flag = json_data.get('flag')

                    if flag == "request":
                        self.request_queue.put([clock,target_client_id,file_id,chunk_id])
                        print(f"Clock [0]:Receive [Request] from server chunk{chunk_id} of file{file_id}")
                    elif flag == "response":
                        file_chunks[(file_id,chunk_id)] = chunk_data
                        print(f"Clock [0]:Receive [data] from server chunk{chunk_id} of file{file_id}")
                except json.JSONDecodeError as e:
                    print("Error to receive : {e}")

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