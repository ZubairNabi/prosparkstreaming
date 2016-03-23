package org.apress.prospark

import com.esotericsoftware.kryo.{KryoSerializable,Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}

class ProtonFlux(
    var year: Int,
    var bin0_57to1_78: Double,
    var bin3_40to17_6: Double,
    var bin22_0to31_0: Double,
    var bin1_894to2_605: Double,
    var bin4_200to6_240: Double,
    var bin3_256to8_132: Double,
    var bin3_276to8_097: Double,
    var bin6_343to42_03: Double,
    var bin17_88to26_81: Double,
    var bin30_29to69_47: Double,
    var bin132_8to242_0: Double
  ) extends KryoSerializable {
  
  def this(year: String, bin0_57to1_78: String, bin3_40to17_6: String, 
      bin22_0to31_0: String, bin1_894to2_605: String, bin4_200to6_240: String, 
      bin3_256to8_132: String, bin3_276to8_097: String, bin6_343to42_03: String,
      bin17_88to26_81: String, bin30_29to69_47: String, bin132_8to242_0: String) {
    this(year.toInt, bin0_57to1_78.toDouble, bin3_40to17_6.toDouble,
        bin22_0to31_0.toDouble, bin1_894to2_605.toDouble, bin4_200to6_240.toDouble, 
        bin3_256to8_132.toDouble, bin3_276to8_097.toDouble, bin6_343to42_03.toDouble,
        bin17_88to26_81.toDouble, bin30_29to69_47.toDouble, bin132_8to242_0.toDouble)
  }
  
  def isSolarStorm = (bin0_57to1_78 > 1.0 || bin3_40to17_6 > 1.0 
    || bin22_0to31_0 > 1.0 || bin1_894to2_605 > 1.0 || bin4_200to6_240 > 1.0 
    || bin3_256to8_132 > 1.0 || bin3_276to8_097 > 1.0 || bin6_343to42_03 > 1.0
    || bin17_88to26_81 > 1.0 || bin30_29to69_47 > 1.0 || bin132_8to242_0 > 1.0)

  override def write(kryo: Kryo, output: Output) {
    output.writeInt(year)
    output.writeDouble(bin0_57to1_78)
    output.writeDouble(bin3_40to17_6)
    output.writeDouble(bin22_0to31_0)
    output.writeDouble(bin1_894to2_605)
    output.writeDouble(bin4_200to6_240)
    output.writeDouble(bin3_256to8_132)
    output.writeDouble(bin3_276to8_097)
    output.writeDouble(bin6_343to42_03)
    output.writeDouble(bin17_88to26_81)
    output.writeDouble(bin30_29to69_47)
    output.writeDouble(bin132_8to242_0)
  }

  override def read(kryo: Kryo, input: Input) {
    year = input.readInt()
    bin0_57to1_78 = input.readDouble()
    bin3_40to17_6 = input.readDouble()
    bin22_0to31_0 = input.readDouble()
    bin1_894to2_605 = input.readDouble()
    bin4_200to6_240 = input.readDouble()
    bin3_256to8_132 = input.readDouble()
    bin3_276to8_097 = input.readDouble()
    bin6_343to42_03 = input.readDouble()
    bin17_88to26_81 = input.readDouble()
    bin30_29to69_47 = input.readDouble()
    bin132_8to242_0 = input.readDouble()
  }

}