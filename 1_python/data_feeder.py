with open("rawMessageSample") as enc_msgs:
    results = []
    i = 1
    for msg in enc_msgs:
        i += 1
        print(str(i) + "," + msg[:-1])

        if i > 100:
            break