import socket
import random

class Node:
    """노드를 생성하여 트리를 구성합니다."""
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

class Server:
    def __init__(self, host="127.0.0.1", port=12345, max_clients=4):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.connected_clients = []
        self.client_id = 1
        self.server_socket = None

    def Server_start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        print(f"서버가 시작되었습니다. 최대 {self.max_clients}개의 클라이언트와 연결할 수 있습니다.")

    def wait_for_all_clients(self):
        while len(self.connected_clients) < self.max_clients:
            client_socket, address = self.server_socket.accept()
            print(f"클라이언트가 연결되었습니다: {address}")
            client_socket.sendall(str(self.client_id).encode())
            print(f"클라이언트 {self.client_id}에게 ID를 전송했습니다.")
            self.connected_clients.append((client_socket, self.client_id))
            self.client_id += 1
        print("모든 클라이언트가 연결되었습니다. 데이터 전송을 시작합니다.")

    def notify_clients_ready(self):
        for client_socket, client_id in self.connected_clients:
            client_socket.sendall("READY".encode())

    def communicate_with_all_clients(self):
        while self.connected_clients:
            random.shuffle(self.connected_clients)
            for client_socket, client_id in self.connected_clients:
                try:
                    data = client_socket.recv(1024).decode() # 큐에서 꺼냄 
                    if not data:
                        self.connected_clients.remove((client_socket, client_id))
                        print(f"클라이언트 {client_id}와의 연결이 종료되었습니다.")
                    else:
                        print(f"클라이언트 {client_id}에서 받은 수식: {data}")
                        
                        # 중위 표기법을 후위 표기법으로 변환하고 출력하여 확인
                        postfix_expression = self.infix_to_postfix(data)
                        print("후위 표기법:", ' '.join(postfix_expression))
                        
                        # 후위 표기법을 트리로 변환
                        tree = self.create_parsing_tree(postfix_expression)
                        print("파싱 트리:")
                        self.print_tree(tree)
                        
                        # 리프 노드 개수 세기
                        leaf_count = self.count_leaf_nodes(tree)
                        print(f"리프 노드 개수: {leaf_count}")
                        
                        # 후위 순회 방식으로 계산
                        result = self.evaluate_postorder(tree)
                        print(f"수식 계산 결과: {result}")
                except ConnectionResetError:
                    self.connected_clients.remove((client_socket, client_id))
                    print(f"클라이언트 {client_id}와의 연결이 비정상적으로 종료되었습니다.")

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

if __name__ == "__main__":
    server = Server()
    server.Server_start()
    server.wait_for_all_clients()
    server.notify_clients_ready()
    server.communicate_with_all_clients()
    input()