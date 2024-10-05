## Cache Server 및 Multi-Thread를 이용한 파일 다운로드 가상 환경 구현

## 조원 및 역할
20193080 이송헌
-코드 작성
-리팩토링
-알고리즘 아이디어 제공
20203017 박재욱
-코드 작성
-동영상 제작
-파일들 작성
-제출
20203100 최원겸
-코드 작성
-동영상 업로드

## 프로그램 구성 요소

## 소스코드 컴파일방법 명시

## 프로그램 실행환경 및 실행방법 설명
실행환경: Python 3.8

실행방법
aws 외부서버에 data server와 cache server 2개 총 3개의 서버를 실행하고
로컬환경에 4개의 client 를 실행시킨다.

## 규칙 및 알고리즘 설명
규칙
0. client가 data server에서 다운을 받는 상태가 아니라면 무조건 하나의 파일을 data server로부터 다운로드 한다.
1. client가 data server에 다운받을 파일을 요청한다
2. data server는 2개의 cache server에서 파일이 있는지 확인을 한다.
	2-1 파일이 존재할 때 client가 해당 cache server에 다운받도록 지시한다.

	2-2 파일이 존재하지 않을 때 2개의 cache server 중 용량이 더 남아있는 쪽에 다운이 가능한지 확인한다
		2-2-1 다운이 가능하면 해당 cache server로 파일을 전송한다.
		2-2-2 다운이 가능하지 않으면 cache server에 있는 파일중 하나를 지우고 파일을 전송한다.
3. cache server에 파일이 존재하지 않고, 다운도 불가능한 상태라면 data server로 직접 다운받도록 한다.
4. 0~3을 반복한다.
알고리즘
ARC 알고리즘

## Error or Additional Message Handling

##Additional Comments
