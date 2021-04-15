package client;

import org.apache.log4j.Logger;
import shared.Util;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class TransactionInterpreter {

    private static class ExecutionContext {
        private final KVStore kvStore;
        private Map<String, String> hashMap = new HashMap<>();

        public ExecutionContext(KVStore kvStore) {
            this.kvStore = kvStore;
        }

        public String getVariable(String identifier) {
            return hashMap.get(identifier);
        }

        public void setVariable(String identifier, String value) {
            if (value == null) {
                hashMap.remove(identifier);
            } else {
                hashMap.put(identifier, value);
            }
        }
    }

    private static abstract class Instruction {
        public abstract String eval(ExecutionContext ctx) throws Exception;
    }

    private static class Constant extends Instruction {

        private String value;

        public Constant(String value) {
            this.value = value;
        }

        @Override
        public String eval(ExecutionContext ctx) {
            return value;
        }
    }

    private static class Variable extends Instruction {

        private String identifier;

        public Variable(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public String eval(ExecutionContext ctx) {
            return ctx.getVariable(identifier);
        }
    }

    private static class Assignment extends Instruction {
        private String identifier;
        private Instruction value;

        public Assignment(String identifier, Instruction value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public String eval(ExecutionContext ctx) throws Exception {
            ctx.setVariable(identifier, value.eval(ctx));
            return null;
        }
    }

    private static class InfixArithmetic extends Instruction {

        public static Set<String> VALID_OPERATORS = new HashSet<>(
                Arrays.asList("+", "-", "*", "/"));

        private Instruction leftOperand;
        private String operation;
        private Instruction rightOperand;

        public InfixArithmetic(Instruction leftOperand,
                               String operation,
                               Instruction rightOperand) {
            this.leftOperand = leftOperand;
            this.operation = operation;
            this.rightOperand = rightOperand;

            if (!VALID_OPERATORS.contains(operation)) {
                throw new IllegalArgumentException(
                        String.format("Invalid operator: %s", operation));
            }
        }

        @Override
        public String eval(ExecutionContext ctx) throws Exception {
            String op1String = "", op2String = "";
            op1String = leftOperand.eval(ctx);
            op2String = rightOperand.eval(ctx);
            // TODO catch errors for these?
            int op1, op2;
            try {
                op1 = Integer.parseInt(op1String);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Operand \"%s\" to \"%s\" is not an integer",
                                op1String, operation));
            }
            try {
                op2 = Integer.parseInt(op2String);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Operand \"%s\" to \"%s\" is not an integer",
                                op2String, operation));
            }
            switch (operation) {
                case "+":
                    return Integer.toString(op1 + op2);
                case "-":
                    return Integer.toString(op1 - op2);
                case "*":
                    return Integer.toString(op1 * op2);
                case "/":
                    return Integer.toString(op1 / op2);
                default:
                    throw new IllegalArgumentException(
                            String.format("Invalid operator: %s", operation));
            }
        }
    }

    private static class GetRequest extends Instruction {
        private String key;
        private String defaultValue;

        public GetRequest(String key, String defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        @Override
        public String eval(ExecutionContext ctx) throws Exception {
            // TODO
            KVMessage message = ctx.kvStore.transactionGet(key);
            if (message.getStatus() == KVMessage.StatusType.GET_ERROR) {
                return defaultValue;
            } else if (message.getStatus() ==
                    KVMessage.StatusType.GET_SUCCESS) {
                return message.getValue();
            } else {
                throw new IllegalStateException(
                        "Invalid return message: " + message.toString());
            }
        }
    }

    private static class PutRequest extends Instruction {
        private static Set<KVMessage.StatusType> VALID_RESPONSE_STATUSES = new HashSet<>(
                Arrays.asList(KVMessage.StatusType.PUT_UPDATE,
                        KVMessage.StatusType.PUT_SUCCESS,
                        KVMessage.StatusType.DELETE_SUCCESS,
                        KVMessage.StatusType.DELETE_ERROR));

        private String key;
        private Instruction value;

        public PutRequest(String key, Instruction value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String eval(ExecutionContext ctx) throws Exception {
            String valueString = value == null ? null : value.eval(ctx);
            KVMessage message = ctx.kvStore.transactionPut(key, valueString);
            if (!VALID_RESPONSE_STATUSES.contains(message.getStatus())) {
                throw new IllegalStateException(
                        "Invalid return message: " + message.toString());
            }
            return null;
        }
    }

    private static class Print extends Instruction {
        private Instruction value;

        public Print(Instruction value) {
            this.value = value;
        }

        @Override
        public String eval(ExecutionContext ctx) throws Exception {
            String valueString = value.eval(ctx);
            System.out.println(valueString);
            return null;
        }
    }

    private static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }

    private static Logger logger = Logger.getRootLogger();

    private static final String PROMPT = ".. ";

    private List<Instruction> instructions = new ArrayList<>();

    private Instruction parseExpression(String[] tokens,
                                        int start,
                                        int end) throws ParseException {
        int numTokens = end - start;
        try {
            if (numTokens == 0) {
                throw new ParseException("Empty statement");

            } else if (numTokens == 3 && InfixArithmetic.VALID_OPERATORS
                    .contains(tokens[start + 1])) {
                return new InfixArithmetic(
                        parseExpression(tokens, start, start + 1),
                        tokens[start + 1],
                        parseExpression(tokens, start + 2, start + 3));

            } else if (numTokens == 1 && tokens[start].charAt(0) == '"' &&
                    tokens[start].charAt(tokens[start].length() - 1) == '"') {
                return new Constant(
                        tokens[start].substring(1, tokens[start].length() - 1));

            } else if (numTokens == 1 && tokens[start].charAt(0) == '$') {
                return new Variable(tokens[start].substring(1));

            } else if (tokens[start].equals("get")) {
                // For convenience, GET will consume all tokens afterwards
                if (numTokens == 4 && tokens[start + 2].equals("default")) {
                    String key = tokens[start + 1];
                    String defaultValue = tokens[start + 3];
                    return new GetRequest(key, defaultValue);
                } else if (numTokens == 2) {
                    String key = tokens[start + 1];
                    return new GetRequest(key, null);
                } else {
                    throw new ParseException(
                            "Invalid number of arguments to GET");
                }

            } else if (numTokens == 1) {
                return new Constant(tokens[start]);

            } else {
                throw new ParseException("Invalid expression");
            }
        } catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    private Instruction parseStatement(String[] tokens,
                                       int start,
                                       int end) throws
            ParseException {
        int numTokens = end - start;
        try {
            if (numTokens == 0) {
                throw new ParseException("Empty statement");

            } else if (numTokens >= 2 && tokens[start].charAt(0) == '$' &&
                    tokens[start + 1].equals("=")) {
                Instruction value = parseExpression(tokens, start + 2, end);
                return new Assignment(tokens[start].substring(1), value);

            } else if (numTokens >= 1 && tokens[start].equals("get")) {
                throw new ParseException(
                        "Return value of get must be assigned to a variable");

            } else if (numTokens >= 1 && tokens[start].equals("print")) {
                Instruction value = parseExpression(tokens, start + 1, end);
                return new Print(value);

            } else if (numTokens >= 3 && tokens[start].equals("put")) {
                String key = tokens[start + 1];
                Instruction value = parseExpression(tokens, start + 2, end);
                return new PutRequest(key, value);

            } else if (numTokens == 2 && tokens[start].equals("put")) {
                String key = tokens[start + 1];
                return new PutRequest(key, null);

            } else {
                throw new ParseException("Invalid statement");
            }
        } catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    public TransactionRunner parse() {
        instructions = new ArrayList<>();
        BufferedReader stdin = new BufferedReader(
                new InputStreamReader(System.in));
        System.out.println("(Type \"help\" for help)");
        while (true) {
            System.out.print(PROMPT);
            try {
                String[] tokens = stdin.readLine().trim().split("\\s+");
                try {
                    if (tokens.length == 1 && tokens[0].equals("end")) {
                        return getFunction(instructions);
                    } else if (tokens.length == 1 && tokens[0].equals("help")) {
                        printHelpMessage();
                    } else if (tokens.length == 1 &&
                            tokens[0].equals("abort")) {
                        instructions.clear();
                        return null;
                    } else {
                        Instruction statement = parseStatement(tokens, 0,
                                tokens.length);
                        instructions.add(statement);
                    }
                } catch (ParseException e) {
                    System.out.printf("ERROR: %s\n", e.getMessage());
                }
            } catch (Exception e) {
                System.out.printf("ERROR: %s\n", Util.getStackTraceString(e));
                logger.error(e);
                return null;
            }
        }
    }

    private void printHelpMessage() {
        System.out.println(
                "Transaction mode: defines and executes a transaction. You can enter commands and statements. Statements are recorded and evaluated when the transaction is executed.");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("- help: display this message.");
        System.out
                .println("- end: finish defining the transaction.");
        System.out.println("- abort: quit transaction mode.");
        System.out.println();
        System.out.println("Statements:");
        System.out.println(
                "- $<variable> = <expression>: assign the value of the expression to the variable.");
        System.out.println("    Expressions can be one of the following:");
        System.out.println("    - <value>: representing a string value.");
        System.out.println(
                "        If starting with $, will evaluate to the value of a variable");
        System.out.println(
                "        If surrounded by \"\", will evaluate to the enclosed string");
        System.out.println(
                "        Otherwise, will evaluate to the token string itself");
        System.out.println(
                "    - <value> <op> <value>: perform integer operation.");
        System.out.println(
                "        <op> can be +, -, *, or / (integer division).");
        System.out.println(
                "        Operands must be strings containing integer.");
        System.out.println(
                "        Result will be a string containing integer.");
        System.out.println(
                "    - get <key> [default <default-value>]: get a tuple from the database.");
        System.out.println(
                "        If default value is given, will use the value if the tuple does not exist.");
        System.out.println("    Examples:");
        System.out.println("    - $a = get a");
        System.out.println("    - $b = $a + 2");
        System.out.println("    - $c = get c default 10");
        System.out.println(
                "- put <key> [<expression>]: set the value of a tuple in the database to the value of the expression.");
        System.out.println(
                "    If value is not given, delete the tuple from the database.");
        System.out.println("    Examples:");
        System.out.println("    - put a 1");
        System.out.println("    - put a $a + 10");
        System.out.println("    - put b get a");
    }

    private TransactionRunner getFunction(List<Instruction> instructions) {
        return (KVStore kvStore) -> {
            ExecutionContext ctx = new ExecutionContext(kvStore);
            for (Instruction instruction : instructions) {
                instruction.eval(ctx);
            }
        };
    }
}
