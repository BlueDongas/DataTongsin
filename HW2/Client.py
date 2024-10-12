import socket
import threading
import time
import random
import pickle
import struct

client_id = None
log_file = None

file_list = []  # 다운로드할 리스트
file_list_lock = threading.Lock()

# 데이터를 전송할 때 데이터 크기를 먼저 보내고 해당 크기만큼 데이터를 보냄
def send_result(client_socket, result):
    try:
        # 데이터를 직렬화
        serialized_data = pickle.dumps((client_id, result))
        data_size = len(serialized_data)

        # 데이터 크기를 먼저 전송 (8바이트)
        client_socket.sendall(struct.pack('Q', data_size))
        # 실제 데이터 전송
        client_socket.sendall(serialized_data)
    except Exception as e:
        print(f"Error while sending data: {e}")

# 서버들로부터 정보를 받는 함수
def receive_file(client_socket, server_name):
    try:
        while True:
            # 데이터 크기를 먼저 수신 (8바이트)
            packed_size = client_socket.recv(8)
            if not packed_size:
                print("No size information received. Closing connection.")
                break

            data_size = struct.unpack('Q', packed_size)[0]
            data = b""

            # 데이터 크기만큼 수신
            while len(data) < data_size:
                packet = client_socket.recv(4096)
                if not packet:
                    print("Connection closed while receiving data.")
                    return
                data += packet

            # 직렬화 해제하여 데이터 처리
            receive_result = pickle.loads(data)
            print(f"Received data from {server_name}: {receive_result}")
            
    except socket.timeout:
        print("Connection timed out")
    except Exception as e:
        print(f"Error receiving data: {e}")

# 다운로드할 파일을 요청하는 함수
<<<<<<< HEAD
def request_file(client_socket, request_list, server_name):
=======
def request_file(client_socket,request_list,server_name):
    flag_rq = "request"
>>>>>>> parent of b651c7c (update 주고받기)
    try:
        while True:
            with file_list_lock:
                if not file_list:
                    print("All task complete")
                    break
                file_number = file_list[0]
            if not request_list:
                continue
            try:
                if file_number == request_list[0]:
                    with file_list_lock:
<<<<<<< HEAD
                        file_list.pop(0)  # 보내야할 요소를 전체 리스트에서 뺌
                    print(f"Request file {file_number} to {server_name}")
                    send_result(client_socket, file_number)
                    request_list.pop(0)
                else:
                    continue  # 서버로 보내야할 리스트의 요소가 전체 리스트의 첫 번째 요소와 같아질 때까지 continue
=======
                        file_list.pop(0)
                    print(f"Request file{file_number} to {server_name}")
                    send_result(client_socket,file_number)
                    request_list.pop(0)
                else:
                    continue
>>>>>>> parent of b651c7c (update 주고받기)
            except Exception as e:
                print(f"Failed to send request file {file_number} to {server_name}: {e}")
    except Exception as e:
        print(f"Failed to send request to {server_name}: {e}")

<<<<<<< HEAD
=======

# 서버들로부터 정보를 받는 함수
def receive_file(client_socket,server_name):
    while True: 
        try:
            receive_data = client_socket.recv(1024)
            try:
                receive_result = pickle.loads(receive_data)
                print(f"Received data from {server_name}: {receive_result}")
            except pickle.UnpicklingError as e:
                print(f"Error unpickling data : {e}")
                break
            # 받은 데이터 정리할 코드 필요 

        except socket.timeout:
            print(f"Connection timed out ")
            break
        except Exception as e:
            print("Error receiving data because {e}")
            break
    return 0

>>>>>>> parent of b651c7c (update 주고받기)
def connect_to_data_server(server_address, server_port, server_name, request_list):
    global client_id
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(10)
        client_socket.connect((server_address, server_port))

        client_id = pickle.loads(client_socket.recv(4096))

        print(f"Connected to {server_name} on port {server_port}")

        request_thread = threading.Thread(target=request_file, args=(client_socket, request_list, server_name))
        receive_thread = threading.Thread(target=receive_file, args=(client_socket, server_name))

        request_thread.start()
        receive_thread.start()
        request_thread.join()
        receive_thread.join()
    finally:
        client_socket.close()
        print(f"Connection closed for client")

def connect_to_cache_server(server_address, server_port, server_name, request_list):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_address, server_port))
        print(f"Connected to {server_name} on port {server_port}")
        time.sleep(10)

        request_thread = threading.Thread(target=request_file, args=(client_socket, request_list, server_name))
        receive_thread = threading.Thread(target=receive_file, args=(client_socket, server_name))

        request_thread.start()
        receive_thread.start()
        request_thread.join()
        receive_thread.join()
    finally:
        client_socket.close()
        print(f"Connection closed for client")

# 클라이언트가 데이터 서버와 2개의 캐시 서버에 각각 연결
def client():
    # 각 서버에 대한 정보를 설정
    Data_server = ('localhost', 10000, 'Data Server')
    Even_Cache_server = ('localhost', 20000, 'Cache Server 1')
    Odd_Cache_server = ('localhost', 30000, 'Cache Server 2')

    Odd_list = []  # 홀수
    Even_list = []  # 짝수 
    Data_request_list = []  # 데이터 서버로부터 요청받을 파일들 

    odd_cache_sum = 0
    even_cache_sum = 0
    data_sum = 0

    for _ in range(10):  # 테스트용 나중에 1000개로 수정
        file_number = random.randint(1, 10000)

        if file_number % 2 == 0:
            if even_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else :
                Even_list.append(file_number)
                even_cache_sum += file_number
        else:
            if odd_cache_sum > data_sum * 1.21 / 2:
                Data_request_list.append(file_number)
                data_sum += file_number
            else :
                Odd_list.append(file_number)
                odd_cache_sum += file_number

        file_list.append(file_number)

    Data_thread = threading.Thread(target=connect_to_data_server, args=(Data_server[0], Data_server[1], Data_server[2], Data_request_list))
    Even_Cache_thread = threading.Thread(target=connect_to_cache_server, args=(Even_Cache_server[0], Even_Cache_server[1], Even_Cache_server[2], Even_list))
    Odd_Cache_thread = threading.Thread(target=connect_to_cache_server, args=(Odd_Cache_server[0], Odd_Cache_server[1], Odd_Cache_server[2], Odd_list))
    
    Data_thread.start()
    Even_Cache_thread.start()
    Odd_Cache_thread.start()

    Data_thread.join()
    Odd_Cache_thread.join()
    Even_Cache_thread.join()

if __name__ == "__main__":
    client()
