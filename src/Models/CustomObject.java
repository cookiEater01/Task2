package Models;

public class CustomObject implements Comparable<CustomObject> {
    String match_id;
    int market_id;
    String outcome_id;
    String specifiers;

    public CustomObject(String match_id, String market_id, String outcome_id, String specifiers) {
        this.match_id = match_id;
        this.market_id = Integer.parseInt(market_id);
        this.outcome_id = outcome_id;
        this.specifiers = specifiers;
    }

    public String getMatch_id() {
        return match_id;
    }

    public int getMarket_id() {
        return market_id;
    }

    public String getOutcome_id() {
        return outcome_id;
    }

    public String getSpecifiers() {
        return specifiers;
    }

    @Override
    public int compareTo(CustomObject o) {
        return this.getMatch_id().compareTo(o.getMatch_id());
    }
}
