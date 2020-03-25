{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Avro.ReuseFixedSpec
where

import Avro.Data.FixedTypes
import Data.Avro.Deriving

import Avro.TestUtils (roundtrip)
import Test.Hspec

{-# ANN module ("HLint: ignore Redundant do"        :: String) #-}

spec :: Spec
spec = describe "Avro.ReuseFixedSpec" $ do
  it "should roundtrip fixed type" $
    let msg = ReuseFixed (FixedData "ABCDEFGHIJKLMNOP") (FixedData "PONMLKJIHGFEDCBA")
    in roundtrip schema'ReuseFixed msg `shouldBe` pure msg

