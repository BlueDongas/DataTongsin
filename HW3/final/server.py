import socket
import queue
import threading
import random
from concurrent.futures import ThreadPoolExecutor
import json


class Clock:
    def __init__(self):
        self.master_clock = 0

class Node:
    """노드를 생성하여 트리를 구성합니다."""
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

class Server:
    def __init__(self, host, port, max_clients):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = {} # 연결된 클라이언트 소켓과 ID를 저장할 리스트
        self.client_id = 1

        self.task_queue =queue.Queue(maxsize=30) #heap 메모리에 위치 최대 사이즈가 30인 작업 대기 큐
        self.result_queue = queue.Queue() # heap 메모리에 위치하는 결과 저장 큐
        self.semaphore = threading.Semaphore(1)

        self.executor = ThreadPoolExecutor(max_workers=200)

    def infix_to_postfix(self, expression):
        """중위 표기법을 후위 표기법으로 변환"""
        precedence = {'+': 1, '-': 1, '*': 2, '/': 2}
        output = []
        operators = []
        for token in self.tokenize(expression):
            if token.isdigit():
                output.append(token)
            elif token in precedence:
                while (operators and operators[-1] in precedence and
                       precedence[operators[-1]] >= precedence[token]):
                    output.append(operators.pop())
                operators.append(token)
        while operators:
            output.append(operators.pop())
        return output

    def create_parsing_tree(self, postfix_tokens):
        """후위 표기법을 파싱 트리로 변환"""
        stack = []
        for token in postfix_tokens:
            if token.isdigit():
                stack.append(Node(int(token)))
            else:
                right = stack.pop()
                left = stack.pop()
                node = Node(token)
                node.left = left
                node.right = right
                stack.append(node)
        return stack.pop() if stack else None

    def count_leaf_nodes(self, node):
        """트리에서 리프 노드 개수를 세는 함수"""
        if node is None:
            return 0
        if node.left is None and node.right is None:
            return 1  # 리프 노드인 경우
        return self.count_leaf_nodes(node.left) + self.count_leaf_nodes(node.right)

    def tokenize(self, expression):
        """수식을 토큰으로 분리"""
        tokens, num = [], ''
        for char in expression:
            if char.isdigit():
                num += char
            else:
                if num:
                    tokens.append(num)
                    num = ''
                if char in "+-*/":
                    tokens.append(char)
        if num:
            tokens.append(num)
        return tokens

    def evaluate_postorder(self, node):
        """후위 순회로 계산"""
        if node is None:
            return 0
        if isinstance(node.value, str) and node.value in "+-*/":
            left_val = self.evaluate_postorder(node.left) if node.left else 0
            right_val = self.evaluate_postorder(node.right) if node.right else 0
            return self.apply_operator(left_val, right_val, node.value)
        return node.value

    def apply_operator(self, left, right, operator):
        """연산자에 따라 계산"""
        operations = {'+': lambda x, y: x + y, '-': lambda x, y: x - y, '*': lambda x, y: x * y, '/': lambda x, y: x / y}
        return operations[operator](left, right)

    def print_tree(self, node, level=0, label="."):
        """트리를 출력하여 구조 확인"""
        if node is not None:
            print(" " * (level * 4) + f"{label}: {node.value}")
            self.print_tree(node.left, level + 1, "L")
            self.print_tree(node.right, level + 1, "R")
    
    def cacluate_task(self,task,client_id, thread_id):
        postfix_expression = self.infix_to_postfix(task)
        return 0
    

    def handle_manage_thread(self):
        while True:
            count = 0
            thread_id = 0
            while count<4:
                is_task = self.task_queue.empty()
                if not is_task :
                    locate_task, client_id = self.task_queue.get()
                    self.executor.submit(self.cacluate_task,locate_task,client_id,thread_id) # 스레드 풀에 작업 할당
                    if(thread_id == 200):
                        thread_id = 0
                    else: thread_id+=1
                else:
                    break
                count +=1

            #결과 전송
            is_result = self.result_queue.empty()
            if is_result:
                client_id, send_result_data = self.result_queue.get()
                
            else:
                continue

    def handle_waiting_thread(self):
        # 모든 클라이언트와 무작위 순서로 통신을 시작
        while self.connected_clients:
            # 매번 무작위로 클라이언트 순서를 섞음
            client_items = list(self.connected_clients.items())
            random.shuffle(client_items)

            for client_id, client_socket in client_items:
                try:
                    # 클라이언트로부터 데이터 수신
                    data = client_socket.recv(4096).decode()
                    if data == "Complete":
                        del self.connected_clients[client_id]
                        print(f"Client {client_id}의 작업이 모두 완료되었습니다.")
                    elif not data:
                        # 클라이언트가 연결을 끊으면 딕셔너리에서 제거
                        del self.connected_clients[client_id]
                        print(f"클라이언트 {client_id}와의 연결이 종료되었습니다.")
                    else:  # 데이터 받는 부분
                        print(f"클라이언트 {client_id}에서 받은 데이터: {data}")

                        # 작업 큐가 다 찼다면 거절, 그렇지 않으면 작업 큐에 저장
                        isfull = self.task_queue.full()
                        if isfull:
                            print(f"task_queue is full, return to Client {client_id}")
                            response_data = json.dumps({"clock": 0, "response": "작업 거절", "task": data, "result": 0})
                            client_socket.sendall(response_data.encode())
                        else:
                            self.task_queue.put((data, client_id))
                except ConnectionResetError:
                    # 클라이언트와의 연결이 끊어졌을 경우 처리
                    del self.connected_clients[client_id]
                    print(f"클라이언트 {client_id}와의 연결이 비정상적으로 종료되었습니다.")


    def wait_for_all_clients(self):
        # 모든 클라이언트가 연결될 때까지 기다림
        while len(self.connected_clients) < self.max_clients:
            client_socket, address = self.server_socket.accept()
            print(f"클라이언트가 연결되었습니다: {address}")

            # 클라이언트에게 ID 전송
            client_socket.sendall(str(self.client_id).encode())
            print(f"클라이언트 {self.client_id}에게 ID를 전송했습니다.")

            # 연결된 클라이언트 목록에 추가
            self.connected_clients[self.client_id] = client_socket

            # 다음 클라이언트 ID로 증가
            self.client_id += 1

        print("모든 클라이언트가 연결되었습니다. 동작을 시작합니다.")
        self.notify_clients_ready()

    def notify_clients_ready(self):
        # 모든 클라이언트에게 준비 완료 신호를 전송
        for client_id, client_socket in self.connected_clients.items():
            client_socket.sendall("READY".encode())

    def Server_start(self):
        self.server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host,self.port))
        self.server_socket.listen(4)
        print(f"서버가 시작되었습니다. 최대 {self.max_clients}개의 클라이언트와 연결할 수 있습니다.")

        self.wait_for_all_clients()

        waiting_thread = threading.Thread(target=self.handle_waiting_thread)
        manage_thread = threading.Thread(target=self.handle_manage_thread)

        waiting_thread.start()
        manage_thread.start()

        waiting_thread.join()
        manage_thread.join()
    

if __name__ == "__main__":
    server = Server(host="127.0.0.1",port=8888,max_clients=4)
    server.Server_start()
    input()