import socket
import time
import threading

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect_to_server(self):
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.host, self.port))
            print("서버에 연결되었습니다.")
            return True
        except Exception as e:
            print(f"서버에 연결할 수 없습니다: {e}")
            return False

    def send_task(self, expression):
        try:
            self.client_socket.sendall(expression.encode())
            print(f"수식 전송: {expression}")

            result = self.client_socket.recv(1024).decode()
            print(f"수신된 결과: {result}")
        except Exception as e:
            print(f"작업 요청 중 오류 발생: {e}")

    def disconnect(self):
        self.client_socket.close()
        print("서버와의 연결이 종료되었습니다.")

def send_periodic_tasks(client, expressions):
    for expr in expressions:
        client.send_task(expr)
        time.sleep(0.001)  

if __name__ == "__main__":
    host = '127.0.0.1'  
    port = 12345        

    client = Client(host, port)
    if client.connect_to_server():
        expressions = ["2+3*4", "5-2/1", "6*7-2"]


        client_thread = threading.Thread(target=send_periodic_tasks, args=(client, expressions))
        client_thread.start()
        client_thread.join()

        # 연결 종료
        client.disconnect()
