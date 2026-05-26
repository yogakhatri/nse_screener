import unittest

from engine.horizon_weights import get_opportunity_weights, normalize_horizon


class HorizonWeightTests(unittest.TestCase):
    def test_weights_sum_to_one(self) -> None:
        for horizon in ("6m", "1y", "3y", "5y", "10y"):
            weights = get_opportunity_weights("neutral", horizon)
            self.assertAlmostEqual(sum(weights.values()), 1.0, places=4)

    def test_5y_emphasizes_growth_and_profitability(self) -> None:
        w5 = get_opportunity_weights("neutral", "5y")
        w6 = get_opportunity_weights("neutral", "6m")
        self.assertGreater(w5["growth"], w6["growth"])
        self.assertGreater(w5["profitability"], w6["profitability"])

    def test_invalid_horizon_raises(self) -> None:
        with self.assertRaises(ValueError):
            normalize_horizon("20y")


if __name__ == "__main__":
    unittest.main()
