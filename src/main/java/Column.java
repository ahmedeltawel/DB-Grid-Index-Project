
import java.io.Serializable;

public class Column implements Serializable {
    public String name;
    public boolean isPK = false;
    public boolean indexed = false;
    public Comparable MaxValue;
    public Comparable MinValue;

    public String strType;

    public Column(String name, String Type) {
        this.name = name;
        this.strType = Type.toLowerCase();
    }
    static int registerFile [] = {22,16,87,54,102,434,33,67,98,49};
    static int instructionMemory[] = {-868021953,472060990};
    static int pc = 0;
    
    public static void fetch() {
        
        int instruction =instructionMemory[pc] ;
        
        // Complete the fetch() body...
        
        decode(instruction);
        
        pc++;
        // Complete the fetch() body...
        
    }


    public static void decode(int instruction) {

        int opcode = 0; // bits31:26
        int rs = 0; // bits25:21
        int rt = 0; // bit20:16
        int rd = 0; // bits15:11
        int shamt = 0; // bits10:6
        int funct = 0; // bits5:0
        int imm = 0; // bits15:0
        int address = 0; // bits25:0

        int valueRS = 0;
        int valueRT = 0;

        opcode = binaryCut(instruction, 26, 6);
        rs = binaryCut(instruction, 21, 5);
        rt = binaryCut(instruction, 16, 5);
        rd = binaryCut(instruction, 11, 5);
        shamt = binaryCut(instruction, 6, 5);
        funct = binaryCut(instruction, 0, 6);
        imm = binaryCut(instruction, 0, 16);
        address = binaryCut(instruction, 0, 26);

        valueRS = registerFile[rs];
        valueRT = registerFile[rt];

        System.out.println("Instruction " + pc);
        System.out.println("opcode = " + opcode);
        System.out.println("rs = " + rs);
        System.out.println("rt = " + rt);
        System.out.println("rd = " + rd);
        System.out.println("shift amount = " + shamt);
        System.out.println("function = " + funct);
        System.out.println("immediate = " + imm);
        System.out.println("address = " + address);
        System.out.println("value[rs] = " + valueRS);
        System.out.println("value[rt] = " + valueRT);
        System.out.println("----------");

    }

    // public static int binaryCut(int binaryNum, int pos, int duration) {
    //     int shiftedNumber = binaryNum >>> pos; // unsined Shift
    //     int pow = (int) Math.pow(2.0, duration); // cut the lower number
    //     int sliced = shiftedNumber % pow;
    //     return sliced;
    // }
    public static int binaryCut(int binaryNum, int pos, int duration) {
        String binaryString = Long.toBinaryString(binaryNum).substring(32);
        binaryString =  binaryString.substring((31-pos)-duration+1, (31-pos)+1);
        
        return Integer.parseUnsignedInt(binaryString, 2);
    }
    
	public static void main(String[] args) {
		  fetch();
		  fetch();
    
    // Expected output
    
    /* 
11001100010000110000100100111111
00000011111111111111111111111111
11001100010000110000100100111111
00000000000000001111111111111111
00011100001000110001010000111110

110011000000000000000000000
1100110000000000000000000000000
110011 00010 00011 00001 00100 111111
00000000000000000000000 11111 000000

    Instruction 0
    opcode = -13 (signed) or 51 (unsigned)
    rs = 2
    rt = 3
    rd = 1
    shift amount = 4
    function = 63
    immediate = 2367
    address = 4393279
    value[rs] = 87
    value[rt] = 54

    ----------
    
    Instruction 1
    opcode = 7
    rs = 1
    rt = 3
    rd = 2
    shift amount = 16
    function = 62
    immediate = 5182
    address = 2298942
    value[rs] = 16
    value[rt] = 54
    
    ----------
    
    */ 
		
	}

}
