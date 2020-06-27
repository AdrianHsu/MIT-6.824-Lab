# CS188-hw1
private repo

# Setup

```sh
(py3) adrianhsu:~/Desktop
$ git clone --mirror https://github.com/S19-CS188/assignment1-skeleton.git
Cloning into bare repository 'assignment1-skeleton.git'...
remote: Enumerating objects: 15, done.
remote: Total 15 (delta 0), reused 0 (delta 0), pack-reused 15
Unpacking objects: 100% (15/15), done.
(py3) adrianhsu:~/Desktop
$ cd assignment1-skeleton.git/
(py3) adrianhsu:~/Desktop/assignment1-skeleton.git (master)
$ git push --mirror https://github.com/AdrianHsu/CS188-hw1.git
Enumerating objects: 15, done.
Counting objects: 100% (15/15), done.
Delta compression using up to 4 threads
Compressing objects: 100% (14/14), done.
Writing objects: 100% (15/15), 1.37 MiB | 117.00 KiB/s, done.
Total 15 (delta 0), reused 0 (delta 0)
To https://github.com/AdrianHsu/CS188-hw1.git
 * [new branch]      master -> master
(py3) adrianhsu:~/Desktop/assignment1-skeleton.git (master)
$ git remote set-url --push origin https://github.com/AdrianHsu/CS188-hw1.git
(py3) adrianhsu:~/Desktop/assignment1-skeleton.git (master)
$ cd ..
(py3) adrianhsu:~/Desktop
$ git clone https://github.com/AdrianHsu/CS188-hw1
Cloning into 'CS188-hw1'...
remote: Enumerating objects: 15, done.
remote: Counting objects: 100% (15/15), done.
remote: Compressing objects: 100% (14/14), done.
remote: Total 15 (delta 0), reused 15 (delta 0), pack-reused 0
Unpacking objects: 100% (15/15), done.
(py3) adrianhsu:~/Desktop
$ cd CS188-hw1/
(py3) adrianhsu:~/Desktop/CS188-hw1 (master)
$ ls
src
(py3) adrianhsu:~/Desktop/CS188-hw1 (master)
$ git remote -v
origin	https://github.com/AdrianHsu/CS188-hw1 (fetch)
origin	https://github.com/AdrianHsu/CS188-hw1 (push)
```



## Compile

```sh
(py3) adrianhsu:~/Desktop/CS188-hw1 (master)
$ export GOPATH=$(pwd)
(py3) adrianhsu:~/Desktop/CS188-hw1 (master)
$ cd src/ma
-bash: cd: src/ma: No such file or directory
(py3) adrianhsu:~/Desktop/CS188-hw1 (master)
$ cd src/main/
(py3) adrianhsu:~/Desktop/CS188-hw1/src/main (master)
$ go run wc.go master kjv12.txt sequential
# command-line-arguments
./wc.go:15:1: missing return at end of function
./wc.go:21:1: missing return at end of function
```

The compiler produces two errors, because the implementation of the `Map()` and `Reduce()` functions are incomplete.


## Submit

```sh
... Basic Test Passed
... One Failure Passed
... Many Failures Passed
```


