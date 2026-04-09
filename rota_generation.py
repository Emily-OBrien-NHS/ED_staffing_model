import pulp

# How many staff you need each hour (index 0 = midnight)
#WEEKDAY
#cons
#required = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]
#middle tier
#required = [8,	6,	5,	4,	3,	3,	3,	3,	4,	6,	8,	11,	13,	14,	14,	13,	13,	14,	14,	14,	14,	12,	11,	9]
#residents
#required = [6,	6,	5,	4,	4,	4,	4,	4,	3,	3,	3,	4,	5,	6,	7,	7,	7,	8,	8,	8,	7,	7,	7,	7]

#WEEKEND
#cons
#required = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]
#required = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1]
#middle tier
#required = [7, 6, 5, 5, 4, 4, 3, 3, 4, 5, 7, 9, 11, 11, 12, 12, 11, 11, 10, 10, 11, 11, 10, 9]
#residents
required = [7, 7, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 5, 6, 7, 7, 7, 7, 7, 6, 6, 7, 6, 6]

hours = range(24)
shift_length = 8

prob = pulp.LpProblem("rota", pulp.LpMinimize)

# Staff starting at each hour
x = [pulp.LpVariable(f"start_{h}", lowBound=0, cat="Integer") for h in hours]

# Minimise total staff
prob += pulp.lpSum(x)

# Coverage constraints
for h in hours:
    covering = [x[s] for s in hours if s <= h < s + shift_length
                or (s + shift_length > 24 and h < (s + shift_length) % 24)]
    prob += pulp.lpSum(covering) >= required[h]

prob.solve(pulp.PULP_CBC_CMD(msg=0))

print(f"Total staff needed: {int(pulp.value(prob.objective))}")
for h in hours:
    n = int(pulp.value(x[h]))
    if n > 0:
        print(f"  {n} staff start at {h:02d}:00")
