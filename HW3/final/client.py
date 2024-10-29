import socket
import time
import threading
import json
import heapq

class Client:
    def __init__(self, host='127.0.0.1', port=8888):
        self.host = host
        self.port = port
        self.client_id = None  # 서버로부터 받은 클라이언트 ID
        self.rejected_tasks = []

        self.is_rec_end = False
        self.is_send_end = False
        self.master_clock = 0
        self.log_queue = []

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
                        rejected_task = self.rejected_tasks.pop(0)
                        send_json_data = json.dumps({"clock": self.master_clock, "task": rejected_task, "flag": "None"}) + "\n"
                        self.client_socket.sendall(send_json_data.encode())

                        log_message = f"Clock  [{self.master_clock}]  {filename}의 거절되었던 내용 재전송: {rejected_task}"
                        heapq.heappush(self.log_queue, (self.master_clock, log_message))
                        self.master_clock += 1
                        time.sleep(0.1)

                    # 현재 작업 전송
                    send_json_data = json.dumps({"clock": self.master_clock, "task": task, "flag": "None"}) + "\n"
                    self.client_socket.sendall(send_json_data.encode())
                    log_message = f"Clock  [{self.master_clock}]  {task} 전송"
                    heapq.heappush(self.log_queue, (self.master_clock, log_message))
                    self.master_clock += 1
                    time.sleep(0.3)

                # EOF에 도달한 경우 'Complete' 메시지 전송
                print("모든 작업 전송 완료")
                send_json_data = json.dumps({"clock": 0, "task": "None", "flag": "Complete"})
                self.client_socket.sendall(send_json_data.encode())
                self.is_send_end = True

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

                    if response=="작업 거절":
                        log_message = f"Clock  [{clock}]  작업 거절됨 : {task}"
                        heapq.heappush(self.log_queue, (clock, log_message))
                        self.rejected_tasks.append(task)
                    elif response == "전체 종료":
                        print("모든 작업이 종료되었습니다.")
                        self.is_end = True
                        break
                    else:
                        log_message = f"Clock  [{clock}]  작업 완료 : {task} = {result}"
                        heapq.heappush(self.log_queue, (clock, log_message))
        except Exception as e:
            print(f"Error {e}")


    def disconnect(self):
        self.client_socket.close()
        print("서버와의 연결이 종료되었습니다.")

    def print_log(self):
        while True:
            if self.is_rec_end: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
                input("Press Enter Any key")  # 프로그램이 종료되지 않도록 입력 대기
                # 최종로그 내용 추가 필요
                return
            
            if self.log_queue and (self.log_queue[0][0] <= self.master_clock or self.is_send_end):  # master_clock - 10 보다 작거나 같다면
                _, log_message = heapq.heappop(self.log_queue)  # 해당 값을 pop
                print(log_message)

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
