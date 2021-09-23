def test_func():
    import random

    INTERVAL = 1000

    circle_points = 0
    square_points = 0

    for i in range(INTERVAL ** 2):

        # Randomly generated x and y values from a uniform distribution [-1,1]
        rand_x = random.uniform(-1, 1)
        rand_y = random.uniform(-1, 1)

        # Distance between (x, y) and the origin
        origin_dist = rand_x ** 2 + rand_y ** 2

        # Checking if (x, y) lies inside the circle
        if origin_dist <= 1:
            circle_points += 1

        square_points += 1

        # Estimating value of pi,
        pi = 4 * circle_points / square_points

    return pi
