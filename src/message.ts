export class Message {

    static getMessage(type: string, data: any) {
        return JSON.stringify({ type, data }) 
    }

    static parseMessage(json: Buffer) {
        try {
            const payload = JSON.parse(json.toString());

            console.log(payload);

            if(typeof payload.type !== "string") 
                throw new Error('Payload não está definido');

            return payload;
        } catch (e) {
            throw e;
        }
    }

} 