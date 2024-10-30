import socket
import time
import threading
import json
import heapq

class Log:
    def __init__(self):
        self.log_file = None
        self.file_lock = threading.Lock()
    def log_write(self,event):
        self.log_file
        with self.file_lock:  # 락을 사용하여 동기화
            if self.log_file is not None:
                self.log_file.write(f"{event}\n")
                self.log_file.flush()
            else:
                print("log_file is not initialized") 

class Client:
    def __init__(self, host='127.0.0.1', port=8888):
        self.host = host
        self.port = port
        self.client_id = None  # 서버로부터 받은 클라이언트 ID
        self.rejected_tasks = []
        self.rejected_request = 0
        self.is_rec_end = False
        self.is_send_end = False
        self.master_clock = 0
        self.log_queue = []
        self.count_clock = 0
        self.log = Log()

    def connect_to_server(self):
        # 서버와의 연결 생성
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))

        # 서버로부터 클라이언트 ID 수신
        self.client_id = int(self.client_socket.recv(1024).decode())
        self.log.log_file = open(f"client{self.client_id}_Log.txt","w")
        print("Connect to Server")
        print(f"Receive ID to Server: {self.client_id}")
        self.log.log_write("Connect to Server")
        self.log.log_write(f"Receive ID to Server: {self.client_id}")

        # 서버로부터 "READY" 신호 대기
        ready_signal = self.client_socket.recv(1024).decode()
        if ready_signal == "READY":
            print("Start Send task to Server")
            self.log.log_write("Start Send task to Server")

    def send_file_contents(self):
        filename = f"Expression{self.client_id}.txt"
        try:
            with open(filename, 'r') as file:
                # 파일의 줄을 순회
                for task in file:
                    task = task.strip()
                    
                    # 빈 줄이면 건너뜀
                    if not task:
                        continue

                    # 거절된 작업이 있는 경우 먼저 처리
                    while self.rejected_tasks:
                        self.rejected_request += 1
                        rejected_task = self.rejected_tasks.pop(0)
                        send_json_data = json.dumps({"clock": self.master_clock, "task": rejected_task, "flag": "None"}) + "\n"
                        self.client_socket.sendall(send_json_data.encode())

                        log_message = f"Clock  [{self.master_clock}]  Rejected {filename} is Process Resending: {rejected_task}"
                        heapq.heappush(self.log_queue, (self.master_clock, log_message))
                        self.master_clock += 1
                        time.sleep(0.1)

                    # 현재 작업 전송
                    send_json_data = json.dumps({"clock": self.master_clock, "task": task, "flag": "None"}) + "\n"
                    self.client_socket.sendall(send_json_data.encode())
                    log_message = f"Clock  [{self.master_clock}]  {task} send"
                    heapq.heappush(self.log_queue, (self.master_clock, log_message))
                    self.master_clock += 1
                    time.sleep(0.1)

                # EOF에 도달한 경우 'Complete' 메시지 전송
                print("All Sending task to Server is Complete.")
                time.sleep(0.1)
                send_json_data = json.dumps({"clock": 0, "task": "None", "flag": "Complete"})
                self.client_socket.sendall(send_json_data.encode())
                self.is_send_end = True
                file.close()

        except FileNotFoundError:
            print(f"파일을 찾을 수 없습니다: {filename}")

    def receive_result(self):
        try:
            while True:
                data = self.client_socket.recv(4096).decode()
                for line in data.splitlines():
                    json_data =json.loads(line)

                    clock = json_data.get('clock')
                    response = json_data.get('response')
                    task = json_data.get('task')
                    result = json_data.get('result')
                    operate_time = json_data.get('operate_time')
                    self.count_clock += int(operate_time) 
                    self.count_clock+=2
                    if response=="작업 거절":
                        log_message = f"Clock  [{clock}]  Task Rejected : {task}"
                        heapq.heappush(self.log_queue, (clock, log_message))
                        self.rejected_tasks.append(task)
                    elif response == "전체 종료":
                        print("All task is Complete")
                        self.is_rec_end = True
                        return
                    else:
                        log_message = f"Clock  [{clock}]  Task Complete : {task} = {result:.2f}"
                        heapq.heappush(self.log_queue, (clock, log_message))
        except Exception as e:
            print(f"Error {e}")


    def disconnect(self):
        self.client_socket.close()
        print("Connection to the server has been terminated.")

    def print_log(self):
        while True:
            if self.is_rec_end: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
                print(f"average wait time : {(self.count_clock/1000):.2f}")
                self.log.log_write(f"average wait time : {(self.count_clock/1000):.2f}")
                print(f"rejected request count {self.rejected_request}")
                self.log.log_write(f"rejected request count {self.rejected_request}")
                input("Press Enter Any key Process End")  # 프로그램이 종료되지 않도록 입력 대기
                return
            
            if self.log_queue and (self.log_queue[0][0] <= self.master_clock or self.is_send_end):  # master_clock - 10 보다 작거나 같다면
                _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                print(log_message)
                self.log.log_write(log_message)

if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    send_thread = threading.Thread(target=client.send_file_contents)
    receive_thread = threading.Thread(target=client.receive_result)
    log_thread = threading.Thread(target=client.print_log)

    log_thread.start()
    send_thread.start()
    receive_thread.start()

    log_thread.join()
    send_thread.join()
    receive_thread.join()

    client.disconnect()
    input()

