package ascop.guardrail

default allow := false

# Allow purchase orders under 10,000
allow if {
    input.event_type == "Proposed_Purchase_Order_Event"
    input.amount <= 10000
}

# Allow price changes under 20%
allow if {
    input.event_type == "Proposed_Price_Change_Event"
    input.change_percent <= 20
}

# Allow logistics rerouting for now (simplified)
allow if {
    input.event_type == "Proposed_Reroute_Event"
}
