import subprocess


def run_one_month(month, year):
    collectors = ["rrc10", "rrc00", "route-views2", "route-views.linx"]
    collectorsStr = ""
    for collector in collectors:
        collectorsStr += collector
        if not collector == collectors[-1]:
            collectorsStr += ","

    # Load the ribs/updates data into the kafka topics for one collector
    for collector in collectors:
        cmd = "KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t ribs --collector " + collector + " --year " + str(
            year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)  # returns the exit code in unix
        cmd = "KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t updates --collector " + collector + " --year " + str(
            year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Produce bgpatom and weighted_atom for one collector
    for collector in collectors:
        cmd = "python3 produce_bgpatom.py -c " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)
        cmd = "python3 produce_weightedatom.py -l " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Produce bc_score for all of the collectors
    for collector in collectors:
        cmd = "python3 produce_bcscore.py -c " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Calculate the hegemony scores
    cmd = "python3 produce_hege.py --year " + str(year) + " --month " + str(month) + " --collectors " + collectorsStr
    subprocess.call(cmd, shell=True)

    # download the hege scores for this time period to a csv file
    cmd = "python3 save_hege_scores.py --year " + str(year) + " --month " + str(month)
    subprocess.call(cmd, shell=True)