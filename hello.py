import sys
ans = [
    # 0-system.sql
    "DATABASES",
    "",
    "",
    "",
    """
DATABASES
DB
DB1
DB2
""".strip(),
    "",
    "",
    "",
    """
DATABASES
DB
DB1
DB2
DB3
""".strip(),
    "",
    """
DATABASES
DB
DB2
DB3
""".strip(),
]
if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("read args:", sys.argv[1:], file=sys.stderr, flush=True)
        if "--init" in sys.argv:
            exit(0)
    for e in ans:
        if input().strip() == "exit":
            exit(-1)
        if e:
            print(e)
        print("@done")
    while True:
        if input() == "exit":
            exit(0)
        print("@nothing")
