import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor
import threading
import heapq
import time

server_address = "0.0.0.0"
# 가상의 파일 목록을 저장 (1~10000 파일 번호와 파일 내용)
virtual_files = {i: i for i in range(1, 10001)}  # 파일 번호와 내용이 동일한 가상 파일

#클라이언트 캐시 동시 동작을 위한 변수
connect_count = 0
total_connect = 6
connect_condition = threading.Condition()

End_client_count = 0
End_cache_count = 0
End_client_count_lock = threading.Lock()
End_cache_count_lock = threading.Lock()

# 클락 관리를 위한 변수 및 리스트
master_clock = 0 
clock = 0 
clock_list = [0, 0, 0, 0, 0, 0]
log_queue = []
clock_list_lock = threading.Lock()
log_queue_lock = threading.Lock()

total_file_size = 0
file_size_lock = threading.Lock()

log_file = open("Data_Server.txt","w")
log_file_lock = threading.Lock()  # 로그 파일 접근을 위한 락

def log_write(event):
    global log_file
    with log_file_lock:  # 락을 사용하여 동기화
        if log_file is not None:
            log_file.write(f"{event}\n")
            log_file.flush()
        else:
            print("log_file is not initialized")

# 클라이언트에게 파일 데이터를 전송하는 함수
def send_data(client_socket, data, id):
    try:
        with clock_list_lock:
            send_to_data = pickle.dumps((master_clock,clock_list[id],data))  # 데이터를 직렬화
        data_size = len(send_to_data)
        client_socket.sendall(struct.pack('Q', data_size))  # 데이터 크기 전송
        client_socket.sendall(send_to_data)  # 데이터 전송
    except ConnectionResetError as e:
        print(f"Connection reset: {e}")
    except socket.timeout as e:
        print(f"Socket timed out: {e}")
    except Exception as e:
        print(f"Error while sending data: {e}")

# 클라이언트의 요청을 처리하는 함수
def handle_client(client_socket, client_id):
    global master_clock, End_client_count, total_file_size
    try:
        while True:
            packed_size = b""  # 데이터 크기 수신
            while len(packed_size)<8:
                packet = client_socket.recv(8-len(packed_size))
                if not packet:
                    break  # 클라이언트 연결이 종료되면 루프 탈출
                packed_size+=packet

            data_size = struct.unpack('Q', packed_size)[0]

            received_data = b""
            while len(received_data) < data_size:
                packet = client_socket.recv(4096)  # 데이터를 수신
                received_data += packet

            recieved_master_clock,received_clock,file_number = pickle.loads(received_data)  # 파일 번호를 역직렬화
            if received_clock != 0:
                #모든 작업 종료
                with End_client_count_lock:
                    End_client_count+=1
                with clock_list_lock:
                    clock_list[client_id + 1] = received_clock
                print(f"Client {client_id} JongRyo")
                log_write(f"Client {client_id} JongRyo")
                break
            if file_number in virtual_files:
                # print(f"Client {client_id} requested file {file_number}")
                log_message = f"Clock [{clock_list[client_id + 1]:.2f}]  Client{client_id} requested file {file_number}."
                with log_queue_lock:
                    heapq.heappush(log_queue, (clock_list[client_id + 1], log_message))

                download_time = file_number / 1024
                with file_size_lock:
                    total_file_size += file_number
                with clock_list_lock:
                    clock_list[client_id + 1] += download_time
                    master_clock = min(clock_list)


                # print(f"Sent file {file_number} to client{client_id}")
                with log_queue_lock:
                    log_message = f"Clock [{clock_list[client_id + 1]:.2f}]  Sent file {file_number} to Client{client_id}."
                    heapq.heappush(log_queue, (clock_list[client_id + 1], log_message))
                send_data(client_socket, file_number, client_id + 1)  # 요청된 파일 전송
    except Exception as e:
        print(f"Error handling client {client_id}: {e}")

# 캐시 서버의 요청을 처리하는 함수
def handle_cache(cache_socket, cache_id):
    global master_clock, End_cache_count, total_file_size
    try:
        while True:
            packed_size = b""  # 데이터 크기 수신
            while len(packed_size)<8:
                packet = cache_socket.recv(8-len(packed_size))
                if not packet:
                    break  # 캐시 서버와 연결 종료되면 루프 탈출
                packed_size += packet

            data_size = struct.unpack('Q', packed_size)[0]

            received_data = b""
            while len(received_data) < data_size:
                packet = cache_socket.recv(4096)  # 데이터를 수신
                received_data += packet

            recreceived_master_clock,received_clock,file_number = pickle.loads(received_data)  # 파일 번호를 역직렬화
            if received_clock != 0:
                #모든 작업 종료
                with End_cache_count_lock:
                    End_cache_count+=1
                    print(f"CAche {cache_id} JongRyo")
                break
            if file_number in virtual_files:
                # print(f"Cache Server {cache_id} requested file {file_number}")    
                log_message = f"Clock [{clock_list[cache_id - 1]:.2f}]  Cache Server {cache_id} requested file {file_number}."
                with log_queue_lock:
                    heapq.heappush(log_queue, (clock_list[cache_id - 1], log_message))

                download_time = file_number / 2048
                with file_size_lock:
                    total_file_size += file_number
                with clock_list_lock:
                    clock_list[cache_id - 1] += download_time
                    master_clock = min(clock_list)
                #print(f"Send file {file_number} to Cache{cache_id}")
                log_message = f"Clock [{clock_list[cache_id - 1]:.2f}]  Send file {file_number} to Cache{cache_id}."
                with log_queue_lock:
                    heapq.heappush(log_queue, (clock_list[cache_id - 1], log_message))
                send_data(cache_socket, file_number,cache_id - 1)  # 요청된 파일 전송
    except Exception as e:
        print(f"Error handling cache server {cache_id}: {e}")

def print_log():
    global master_clock, total_file_size
    while True:
        if End_client_count == 4 and End_cache_count == 2: # 모든 작업 수행 시 최종 통계 로그 찍고 함수 종료 코드
            time.sleep(2)
            with log_queue_lock:
                while log_queue:
                    _, log_message = heapq.heappop(log_queue)
                    print(log_message)
                    log_write(log_message)

            with clock_list_lock:
                final_clock = max(clock_list)
            print(f"Final clock [{final_clock}] ")
            log_write(f"Final clock [{final_clock}] ")

            print(f"Average send speed : {total_file_size/final_clock/1024:.02f}Mbps")
            log_write(f"Average send speed : {total_file_size/final_clock/1024:.02f}Mbps")
            # 최종로그 내용 추가 필요

            break
        if log_queue and log_queue[0][0] <= master_clock:  # master_clock보다 작거나 같다면
            with log_queue_lock:
                if log_queue and log_queue[0][0] <= master_clock:
                    _, log_message = heapq.heappop(log_queue)  # 해당 값을 pop
                    print(log_message)
                    log_write(log_message)
                    # 파일에 출력하는 코드 필요

# 데이터 서버를 실행하는 메인 함수
def main():
    global connect_count,server_address
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_address, 6000))  # 데이터 서버 포트 바인딩
    server_socket.listen(6)  # 최대 6개의 클라이언트 연결 대기
    server_socket.settimeout(50)

    print("Data Server started, waiting for connections...")

    # log 출력 스레드
    log_thread = threading.Thread(target=print_log)
    log_thread.start()

    # 클라이언트와 통신하는 스레드 풀 (최대 4개 클라이언트 처리)
    with ThreadPoolExecutor(max_workers=4) as client_executor, \
        ThreadPoolExecutor(max_workers=2) as cache_executor:  # 캐시 서버와 통신하는 스레드 풀 (최대 2개 캐시 서버 처리)
        count = 0
        client_id = 0
        cache_id = 0
        
        try:
            while True:
                try:
                    client_socket, addr = server_socket.accept()  # 클라이언트 또는 캐시 서버 연결 대기
                except Exception as e:
                    print("")
                    break  # 소켓 오류 시 루프 종료

                if count < 2:  # 캐시 서버일 경우
                    cache_id += 1
                    client_socket.sendall(pickle.dumps(cache_id))
                    print(f"Connected to Cache Server {cache_id}")
                    cache_executor.submit(handle_cache, client_socket, cache_id)  # 캐시 서버 처리 스레드
                else:  # 일반 클라이언트일 경우
                    client_id += 1
                    client_socket.sendall(pickle.dumps(client_id))
                    print(f"Connected to Client {client_id}")
                    client_executor.submit(handle_client, client_socket, client_id)  # 클라이언트 처리 스레드
                count += 1
        except Exception as e:
            print("종료")
        finally:
            # 서버 소켓이 종료되면 모든 소켓 닫기
            try:
                server_socket.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                print("모든 프로세스가 종료되었습니다.")
        server_socket.close()


if __name__ == "__main__":
    main()
    input("Press Enter Any key")
    log_file.close()
