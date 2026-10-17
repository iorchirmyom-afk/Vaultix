import { Injectable, Logger, Inject } from '@nestjs/common';
import { ConfigType } from '@nestjs/config';
import stellarConfig from '../config/stellar.config';
import * as StellarSdk from '@stellar/stellar-sdk';
import { retryWithBackoff } from '../utils/retry.util';
import { getCorrelationId } from '../utils/logger.util';
import {
  StellarAccountResponse,
  StellarSubmitTransactionResponse,
  StellarTransactionResponse,
  StellarServer,
} from '../types/stellar.types';

@Injectable()
export class StellarService {
  private readonly logger = new Logger(StellarService.name);
  private server: StellarServer;
  private networkPassphrase: string;

  constructor(
    @Inject(stellarConfig.KEY)
    private config: ConfigType<typeof stellarConfig>,
  ) {
    this.networkPassphrase = this.config.networkPassphrase;
    this.server = new StellarSdk.horizon.Server(this.config.horizonUrl);

    this.logger.log(
      this.withCid(
        `Initialized Stellar service for ${this.config.network} network`,
      ),
    );
    this.logger.log(this.withCid(`Horizon URL: ${this.config.horizonUrl}`));
  }

  /**
   * Retrieves account information from the Stellar network
   * @param publicKey The public key of the account to retrieve
   * @returns Account record with balance and sequence number
   */

  async checkHealth(): Promise<boolean> {
    try {
      await this.server.root();
      return true;
    } catch {
      return false;
    }
  }

  async getAccount(publicKey: string): Promise<StellarAccountResponse> {
    try {
      this.logger.log(this.withCid(`Fetching account info for: ${publicKey}`));

      const account = (await this.server
        .accounts()
        .accountId(publicKey)
        .call()) as unknown as StellarAccountResponse;

      this.logger.log(
        this.withCid(`Successfully retrieved account info for: ${publicKey}`),
      );

      return account;
    } catch (error) {
      this.logger.error(
        this.withCid(
          `Failed to fetch account ${publicKey}: ${this.getErrorMessage(error)}`,
        ),
      );
      throw this.mapStellarError(error, `Error fetching account ${publicKey}`);
    }
  }

  /**
   * Validates an asset against the Stellar Horizon API
   * @param code Asset code
   * @param issuer Asset issuer
   */
  async validateAsset(code: string, issuer: string): Promise<boolean> {
    try {
      this.logger.log(this.withCid(`Validating asset: ${code} from ${issuer}`));
      const assets = await this.server
        .assets()
        .forCode(code)
        .forIssuer(issuer)
        .call();
      return assets.records.length > 0;
    } catch (error) {
      this.logger.error(
        this.withCid(
          `Failed to validate asset ${code}: ${this.getErrorMessage(error)}`,
        ),
      );
      return false;
    }
  }

  /**
   * Builds a transaction with the provided operations
   * @param sourcePublicKey Public key of the source account
   * @param operations Array of operations to include in the transaction
   * @param memo Optional memo for the transaction
   * @param fee Optional fee override (in stroops)
   * @returns Built transaction object
   */
  async buildTransaction(
    sourcePublicKey: string,
    operations: StellarSdk.xdr.Operation[],
    memo?: StellarSdk.Memo,
    fee?: number,
  ): Promise<StellarSdk.Transaction> {
    try {
      this.logger.log(
        this.withCid(`Building transaction for account: ${sourcePublicKey}`),
      );

      // Fetch the account to get the latest sequence number
      const account = await this.getAccount(sourcePublicKey);

      // Calculate fee if not provided (minimum 100 stroops per operation)
      const calculatedFee = fee || Math.max(100, operations.length * 100);

      // Create transaction builder
      const transactionBuilder = new StellarSdk.TransactionBuilder(account, {
        fee: calculatedFee.toString(),
        networkPassphrase: this.networkPassphrase,
      });

      // Add operations to the transaction
      for (const operation of operations) {
        transactionBuilder.addOperation(operation);
      }

      // Add memo if provided
      if (memo) {
        transactionBuilder.addMemo(memo);
      }

      const transaction = transactionBuilder.build();
      this.logger.log(
        this.withCid(
          `Successfully built transaction with hash: ${transaction.hash().toString('hex')}`,
        ),
      );

      return transaction;
    } catch (error) {
      this.logger.error(
        this.withCid(
          `Failed to build transaction for account ${sourcePublicKey}: ${this.getErrorMessage(error)}`,
        ),
      );
      throw this.mapStellarError(
        error,
        `Error building transaction for account ${sourcePublicKey}`,
      );
    }
  }

  /**
   * Submits a transaction to the Stellar network with retry logic
   * @param transaction The transaction object to submit
   * @returns Transaction result
   */
  async submitTransaction(
    transaction: StellarSdk.Transaction,
  ): Promise<StellarSubmitTransactionResponse> {
    try {
      this.logger.log(this.withCid('Submitting transaction with retry logic'));

      const result = await retryWithBackoff(
        async () => {
          const res = await this.server.submitTransaction(transaction, {
            skipMemoRequiredCheck: true,
          });
          return res;
        },
        this.config.maxRetries,
        this.config.retryDelay,
      );

      this.logger.log(
        this.withCid(`Successfully submitted transaction: ${result.hash}`),
      );
      return result;
    } catch (error) {
      this.logger.error(
        this.withCid(
          `Failed to submit transaction after ${this.config.maxRetries + 1} attempts: ${this.getErrorMessage(error)}`,
        ),
      );
      throw this.mapStellarError(
        error,
        `Error submitting transaction after ${this.config.maxRetries + 1} attempts`,
      );
    }
  }

  /**
   * Streams transactions for a given account
   * @param accountId The account ID to stream transactions for
   * @param callback Callback function to handle incoming transactions
   * @returns EventSource object for stream control
   */
  streamTransactions(
    accountId: string,
    callback: (transaction: StellarTransactionResponse) => void,
  ): any {
    this.logger.log(
      this.withCid(`Starting transaction stream for account: ${accountId}`),
    );

    const handler = (transaction: any) => {
      const typedTransaction =
        transaction as unknown as StellarTransactionResponse;
      this.logger.log(
        this.withCid(
          `Received transaction: ${typedTransaction.id} for account: ${accountId}`,
        ),
      );
      callback(typedTransaction);
    };

    // Create event stream for account transactions

    const eventSource = this.server
      .transactions()
      .forAccount(accountId)
      .cursor('now')
      .stream({
        onmessage: handler,
      });

    this.logger.log(
      this.withCid(`Transaction stream established for account: ${accountId}`),
    );
    return eventSource;
  }

  /**
   * Checks the status of a submitted transaction
   * @param transactionHash The hash of the transaction to check
   * @returns Transaction response if found, null otherwise
   */
  async checkTransactionStatus(
    transactionHash: string,
  ): Promise<StellarTransactionResponse | null> {
    try {
      this.logger.log(
        this.withCid(`Checking status for transaction: ${transactionHash}`),
      );

      const transaction = (await this.server
        .transactions()
        .transaction(transactionHash)
        .call()) as unknown as StellarTransactionResponse;

      this.logger.log(
        this.withCid(
          `Transaction ${transactionHash} status: ${transaction.successful ? 'SUCCESS' : 'FAILED'}`,
        ),
      );
      return transaction;
    } catch (error) {
      if (this.isNotFoundError(error)) {
        // Transaction not found (possibly still pending)
        this.logger.log(
          this.withCid(
            `Transaction ${transactionHash} not found (may still be pending)`,
          ),
        );
        return null;
      }

      this.logger.error(
        this.withCid(
          `Failed to check transaction status ${transactionHash}: ${this.getErrorMessage(error)}`,
        ),
      );
      throw this.mapStellarError(
        error,
        `Error checking transaction status ${transactionHash}`,
      );
    }
  }

  /**
   * Validates a Stellar public key
   * @param publicKey The public key to validate
   * @returns True if valid, false otherwise
   */
  isValidPublicKey(publicKey: string): boolean {
    try {
      return StellarSdk.StrKey.isValidEd25519PublicKey(publicKey);
    } catch {
      return false;
    }
  }

  /**
   * Validates a Stellar secret key
   * @param secretKey The secret key to validate
   * @returns True if valid, false otherwise
   */
  isValidSecretKey(secretKey: string): boolean {
    try {
      return StellarSdk.StrKey.isValidEd25519SeedSeed(secretKey);
    } catch {
      return false;
    }
  }

  /**
   * Creates a new Stellar keypair
   * @returns New keypair with public and private keys
   */
  createKeypair(): StellarSdk.Keypair {
    const keypair = StellarSdk.Keypair.random();
    this.logger.log(
      this.withCid(
        `Created new keypair with public key: ${keypair.publicKey()}`,
      ),
    );
    return keypair;
  }

  /**
   * Prefixes a message with the current correlation ID when available.
   * @param message The log message
   * @returns The message with correlation ID attached
   */
  private withCid(message: string): string {
    const cid = getCorrelationId();
    return cid ? `[${cid}] ${message}` : message;
  }

  /**
   * Type guard to check if error has response with status
   */
  private isNotFoundError(error: unknown): boolean {
    /* eslint-disable @typescript-eslint/no-unsafe-member-access */
    return (
      typeof error === 'object' &&
      error !== null &&
      'response' in error &&
      typeof (error as any).response === 'object' &&
      (error as any).response !== null &&
      'status' in (error as any).response &&
      (error as any).response.status === 404
    );
    /* eslint-enable @typescript-eslint/no-unsafe-member-access */
  }

  /**
   * Safely extracts error message from unknown error type
   */
  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    if (typeof error === 'object' && error !== null && 'message' in error) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      return String((error as any).message);
    }
    return 'Unknown error';
  }

  /**
   * Maps Stellar SDK errors to more descriptive error messages
   * @param error The error to map
   * @param defaultMessage Default message if specific mapping isn't found
   * @returns Mapped error
   */
  private mapStellarError(error: unknown, defaultMessage: string): Error {
    if (!error) {
      return new Error(defaultMessage);
    }

    // Type guard for error objects
    if (typeof error === 'object' && error !== null) {
      // Check if it's a Horizon API error
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const errorObj = error as any;
      /* eslint-disable @typescript-eslint/no-unsafe-member-access */
      if (errorObj.response?.data) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const problem = errorObj.response.data;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const title =
          problem.title || problem.extras?.result_codes?.transaction;

        if (problem.detail) {
          return new Error(
            `Stellar API Error: ${String(problem.detail)} (${String(title)})`,
          );
        }

        if (problem.extras?.result_codes) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          const codes = problem.extras.result_codes;
          return new Error(
            `Stellar Transaction Error: ${JSON.stringify(codes)}`,
          );
        }
      }
      /* eslint-enable @typescript-eslint/no-unsafe-member-access */

      // Check for specific Stellar SDK error types
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      if (errorObj.name === 'NetworkError') {
        return new Error(`Stellar Network Error: ${String(errorObj.message)}`);
      }
    }

    return new Error(defaultMessage);
  }
}
