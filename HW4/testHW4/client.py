import socket
import threading
import os
import time
import queue
import json
import base64
import struct
import hashlib
import random
import heapq

TOTAL_CHUNK = None # maybe 3907 정확하게 파일 크기가 512,000,000 byte 기준
CHUNK_SIZE = 128 * 1024 #128kb
BUFFER_SIZE = 1024*150
SLEEP_TIME =0.0000001

send_event=threading.Event()
receive_event=threading.Event()
stop_event = threading.Event()

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

        self.master_clock = 0
        self.master_clock_lock = threading.Lock()
        self.log_queue = []

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
        check_chunk_count = 0
        target_files = self.target_files
        random.shuffle(target_files)
        for file_id in target_files: # B,C,D
            for chunk_id in range(TOTAL_CHUNK): #1,2,3,4,5,...3907
                check_chunk_count+=1

                if (file_id,chunk_id) not in file_chunks:
                    with self.master_clock_lock:
                        clock = self.master_clock

                    json_data={
                        "clock":clock, 
                        "target_client_id":self.client_id,
                        "file_id":file_id, #A
                        "chunk_id":chunk_id,#int 1
                        "chunk_data":"None",
                        "flag":"request"
                    }
                    data_to_send=json.dumps(json_data) + "\n"
                    self.client_socket.sendall(data_to_send.encode())
                    time.sleep(SLEEP_TIME)
                    # print(f"Clock [{clock}]:Request to server chunk{chunk_id} of file{file_id}")
                    log_message = f"Clock [{clock}]:Request to server chunk{chunk_id} of file{file_id}"
                    heapq.heappush(self.log_queue, (clock, log_message))

                for _ in range(10):
                    if not self.request_queue.empty(): #요청 받은 파일 전송
                        clock,target_client_id,send_file_id,send_chunk_id = self.request_queue.get()
                        chunk_data_b64 = base64.b64encode(file_chunks[(send_file_id,send_chunk_id)]).decode('utf-8') #chunk_data base64로 문자열화

                        json_data = {
                            "clock":clock,
                            "target_client_id":target_client_id,
                            "file_id":send_file_id,
                            "chunk_id":send_chunk_id,
                            "chunk_data":chunk_data_b64,
                            "flag":"response"
                        }

                        data_to_send = json.dumps(json_data)+"\n"
                        self.client_socket.sendall(data_to_send.encode())
                        # print(f"Clock [{clock}]:Send to server chunk{send_chunk_id+1} of file{send_file_id}")
                        log_message = f"Clock [{clock}]:Send to server chunk{send_chunk_id+1} of file{send_file_id}"
                        heapq.heappush(self.log_queue, (clock, log_message))
                    time.sleep(SLEEP_TIME/10)
        print("Debug code")
        while not stop_event.is_set():
            if not self.request_queue.empty(): #요청 받은 파일 전송
                clock,target_client_id,send_file_id,send_chunk_id = self.request_queue.get()
                chunk_data_b64 = base64.b64encode(file_chunks[(send_file_id,send_chunk_id)]).decode('utf-8') #chunk_data base64로 문자열화
                json_data = {
                    "clock":clock,
                    "target_client_id":target_client_id,
                    "file_id":send_file_id,
                    "chunk_id":send_chunk_id,
                    "chunk_data":chunk_data_b64,
                    "flag":"response"
                }

                data_to_send = json.dumps(json_data)+"\n"
                self.client_socket.sendall(data_to_send.encode())
                time.sleep(SLEEP_TIME)
                # print(f"Clock [{clock}]:Send to server chunk{send_chunk_id+1} of file{send_file_id}")
                log_message = f"Clock [{clock}]:Send to server chunk{send_chunk_id+1} of file{send_file_id}"
                heapq.heappush(self.log_queue, (clock, log_message))
            else:
                json_data = {"flag":"complete"}
                data_to_send = json.dumps(json_data)
                self.client_socket.sendall(data_to_send.encode())
        print("send 스레드 종료")

        

    def receive_to_server(self):
        buffer = ""
        while not stop_event.is_set():
            data = self.client_socket.recv(BUFFER_SIZE).decode()
            buffer += data
            while '\n' in buffer:
                line, buffer = buffer.split('\n',1)
                try:
                    json_data = json.loads(line)
                    flag = json_data.get('flag')

                    if flag == "complete":
                        print("All request file Receive")
                        stop_event.set()
                        break

                    clock = json_data.get('clock')
                    target_client_id = json_data.get('target_client_id')
                    file_id = json_data.get('file_id')
                    chunk_id = json_data.get('chunk_id')
                    chunk_data = json_data.get('chunk_data')
                    
                    if flag == "request":
                        self.request_queue.put([clock,target_client_id,file_id,chunk_id])
                        # print(f"Clock [{clock}]:Receive [Request] from server chunk{chunk_id} of file{file_id}")
                        log_message = f"Clock [{clock}]:Receive [Request] from server chunk{chunk_id} of file{file_id}"
                        heapq.heappush(self.log_queue, (clock, log_message))
                    elif flag == "response":
                        file_chunks[(file_id,chunk_id)] = chunk_data
                        # print(f"Clock [{clock}]:Receive [data] from server chunk{chunk_id} of file{file_id}")
                        log_message = f"Clock [{clock}]:Receive [data] from server chunk{chunk_id} of file{file_id}"
                        heapq.heappush(self.log_queue, (clock, log_message))

                    with self.master_clock_lock:
                        self.master_clock = clock
                except json.JSONDecodeError as e:
                    print(f"Error to receive : {e}")

    def merge_file(self):
        directory_path = f"./client{self.client_id}"
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        for file_id in ['A','B','C','D']:
            file_path = self.assemble_chunk(file_id)
            if file_path:
                md5_value = self.verify_file_md5(file_path)
                print(f"{file_id}file's md5 is {md5_value}")

    def assemble_chunk(self,file_id):
        file_path = f"./client{self.client_id}/{file_id}"
        with open(file_path,'wb') as f:
            for chunk_id in range(TOTAL_CHUNK):
                chnuk_data_b64 = file_chunks.get((file_id,chunk_id))
                if chnuk_data:
                    chnuk_data = base64.b64decode(chnuk_data_b64)
                    f.write(chnuk_data)
                else:
                    print(f"Missing chunk {chunk_id} in file {file_id}")
                    return False
        print(f"{file_id}file complete assemble chunk as {file_path}")
        return file_path

    def verify_file_md5(self,file_path):
        md5_hash = hashlib.md5()
        with open(file_path,'rb') as f:
            for chunk in iter(lambda:f.read(4096),b""):
                md5_hash.update(chunk)
        final_md5 = md5_hash.hexdigest()
        return final_md5
    
    def disconnect(self):
        return
    
    def print_log(self):
        while True:
            if False: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
                while self.log_queue:
                    _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                    print(log_message)
                # 최종로그 내용 추가 필요


                input("Press Enter Any key")  # 프로그램이 종료되지 않도록 입력 대기
                return
            
            if self.log_queue and (self.log_queue[0][0] <= self.master_clock - 20):  # master_clock - 10 보다 작거나 같다면
                _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                print(log_message)
    
if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    
    send_thread = threading.Thread(target=client.send_to_server)
    receive_thread = threading.Thread(target=client.receive_to_server)
    log_thread = threading.Thread(target=client.print_log)

    log_thread.start()
    send_thread.start()
    receive_thread.start()
    
    log_thread.join()
    send_thread.join()
    receive_thread.join()
                   
    client.merge_file() ## 종료되고 최종적으로 모인 청크 합치기
    print("finish")
    input()