print("Hello World!")
print("Data Pipeline")

name = "Akekachai Vijityannapon"
number = 10
a = 5.5
b = 9
print(name, number, a, b)

c = a + b
c = c * 10
print(c)

print(5 % 4)
print(5 % 2)

if c > 150:
    c = 100
    b = 100
    a = 100
else:
    c = 9
    a = 9
    b = 9

print(a, b, c)

l = [1, 2, 3, 4, 5]
for item in l:
    if item % 2 == 0:
        print(item)
