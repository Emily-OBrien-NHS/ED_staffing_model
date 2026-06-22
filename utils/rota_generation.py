import pulp

def optimal_rota(required_staff, shift_length):
    hours = range(24)
    prob = pulp.LpProblem("rota", pulp.LpMinimize)

    # Staff starting at each hour
    x = [pulp.LpVariable(f"start_{h}", lowBound=0, cat="Integer") for h in hours]

    # Minimise total staff
    prob += pulp.lpSum(x)

    # Coverage constraints
    for h in hours:
        covering = [x[s] for s in hours if s <= h < s + shift_length
                    or (s + shift_length > 24 and h < (s + shift_length) % 24)]
        prob += pulp.lpSum(covering) >= required_staff[h]

    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    
    headline = f"Total staff needed: {int(pulp.value(prob.objective))}"
    outstr = ''
    for h in hours:
        n = int(pulp.value(x[h]))
        if n > 0:
            outstr += f"  {n} staff start at {h:02d}:00 \n"
    return headline, outstr
