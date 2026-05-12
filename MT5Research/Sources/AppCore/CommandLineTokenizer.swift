import Foundation

public enum CommandLineTokenizerError: Error, CustomStringConvertible, Sendable, Equatable {
    case unterminatedQuote(Character)
    case danglingEscape

    public var description: String {
        switch self {
        case .unterminatedQuote(let quote):
            return "Command line has an unterminated \(quote) quote."
        case .danglingEscape:
            return "Command line ends with a dangling escape character."
        }
    }
}

public struct CommandLineTokenizer: Sendable {
    public init() {}

    public func tokenize(_ line: String) throws -> [String] {
        var tokens: [String] = []
        var current = ""
        var quote: Character?
        var escaping = false

        for character in line {
            if escaping {
                current.append(character)
                escaping = false
                continue
            }

            if character == "\\" {
                escaping = true
                continue
            }

            if let activeQuote = quote {
                if character == activeQuote {
                    quote = nil
                } else {
                    current.append(character)
                }
                continue
            }

            if character == "\"" || character == "'" {
                quote = character
                continue
            }

            if character == " " || character == "\t" {
                if !current.isEmpty {
                    tokens.append(current)
                    current.removeAll(keepingCapacity: true)
                }
                continue
            }

            current.append(character)
        }

        if escaping {
            throw CommandLineTokenizerError.danglingEscape
        }
        if let quote {
            throw CommandLineTokenizerError.unterminatedQuote(quote)
        }
        if !current.isEmpty {
            tokens.append(current)
        }
        return tokens
    }
}
