# 6.5840-2024

# Branch Description
- main，include lab1 mapreduce，lab2 kvserver，lab3A voteRequest
- 3B，lab3B，there are some bugs, look 3D branch for details;
- 3C，lab3C, there are some bugs, look 3D branch for details;
- 3D, fix bugs in 3B，3C;
- 3D-fixed, lab3D；
- 4A， lab4A
- 4B   lab4B
- finish all

# Debug
使用6.5840的之前的MIT学生的脚本；


- prettyDebug.go: 日志文件，不用管；
- dslogs.py: 
   VERBOSE=1 go test -run "FuncName" > log      // FuncName:你想要测试的函数名称，test_test.go文件里


   python dslog.py -c 3 log > log.log           // -c: 服务器数量;

- dstest.py:
  python dstest.py -v 1 "TestName" -n 100


 -v: VERBOSE，默认不打印日志，设置为1表示打印日志； -n: 表示测试跑的数量； TestName: 表示你想跑的测试名称，可以是单独的某个函数，也可以是多个函数；比如3A，表示测试所有带有3A的函数；

# Result
![lab3all](https://github.com/user-attachments/assets/9a9311b1-a192-4e0d-a617-0ead47b05089)
![lab4A](https://github.com/user-attachments/assets/3ec083fb-5b81-4bfa-a2e7-fc16b301174a)
![lab4B](https://github.com/user-attachments/assets/4ad6b225-d2bb-4054-a94d-ceb6aa73c082)

# Now
- 3A,3B,3C,3D,4A,4B全部完成
- 目前3A,3B,3C各跑500遍无错；3D跑50遍无措；
- 如有错误，欢迎指出；
- 博客会在近期放出；
