import socket
import threading
import json
import random
import time

connected_clients = {}  # 클라이언트 주소와 포트 정보 저장
count = 0
count_lock = threading.Lock()

server_start_time = time.time()

def get_clock():
    clock = int(time.time() - server_start_time)
    return clock

def handle_client(client_socket, client_address):
    global connected_clients, count
    try:
        # 메시지 수신
        message = client_socket.recv(4096).decode('utf-8')
        message_data = json.loads(message)

        if message_data["type"] == "CONNECT":
            
            clock = get_clock()

            print(f"Clock {clock} : 클라이언트 {client_address} 연결 요청")
            # 새로운 클라이언트에게 고유 포트 할당
            with count_lock:
                count += 1
                client_id = count
                client_port = 50000 + count
            connected_clients[client_address] = {"port": client_port, "id": client_id}
            print(f"클라이언트 {client_id} {client_address}의 고유 포트: {client_port}")

            if len(connected_clients) > 5:
                number_of_connections = 2
            else:
                number_of_connections = 4

            # 연결 가능한 다른 클라이언트 정보 수집 (본인 정보 제외)
            other_clients = random.sample(
                [{"ip": addr[0], "port": info["port"], "id": info["id"]}
                 for addr, info in connected_clients.items() if addr != client_address],
                k=min(number_of_connections, len(connected_clients) - 1)  # 최대 4개
            )
            clock = get_clock()
            response_data = {
                "client_id": client_id,
                "port": client_port,
                "peers": other_clients,
                "clock": clock
            }

            # JSON 형식으로 포트와 다른 클라이언트 정보를 전송
            client_socket.send(json.dumps(response_data).encode('utf-8'))

        elif message_data["type"] == "DISCONNECT":
            disconnecting_id = message_data["client_id"]
            print(f"클라이언트 {disconnecting_id} 종료 요청")

            # 최대 3개의 다른 클라이언트 정보 전송
            remaining_clients = [{"ip": addr[0], "port": info["port"], "id": info["id"]} for addr, info in connected_clients.items() if info["id"] != disconnecting_id][:3]
            client_socket.send(json.dumps(remaining_clients).encode('utf-8'))

            # connected_clients에서 해당 클라이언트 제거
            to_remove = None
            for addr, info in connected_clients.items():
                if info["id"] == disconnecting_id:
                    to_remove = addr
                    break

            if to_remove:
                del connected_clients[to_remove]
                print(f"클라이언트 {disconnecting_id} ({to_remove})가 제거되었습니다.")
        
        elif message_data["type"] == "FIND":
            client_id = message_data["client_id"]
            target_id = message_data["target_id"]
            print(f"클라이언트 {client_id}가 {target_id} 정보를 요청.")

            target_client = None
            for addr, info in connected_clients.items():
                if info["id"] == target_id:
                    target_client = [{"ip": addr[0], "port": info["port"], "id": info["id"]}]
                    break

            if target_client:
                client_socket.send(json.dumps(target_client).encode('utf-8'))
                print(f"클라이언트 {target_id} 정보 전송 완료: {target_client}")
            else:
                client_socket.send(json.dumps({"error": "Client not found"}).encode('utf-8'))
                print(f"클라이언트 {target_id} 정보를 찾을 수 없음")


    except Exception as e:
        print(f"에러 발생: {e}")

    finally:
        client_socket.close()
        print(f"클라이언트 {client_address}와 연결 종료")


def server_program():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', 12345))
    server_socket.listen(5)
    print("서버가 실행 중...")

    while True:
        client_socket, client_address = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, client_address)).start()


if __name__ == "__main__":
    server_program()
